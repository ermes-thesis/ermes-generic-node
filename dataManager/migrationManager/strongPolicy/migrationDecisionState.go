package strongPolicy

import (
	"dataManager/lru"
	gossip "dataManager/messages"
	stack "dataManager/migrationManager/callbackStack"
	usage "dataManager/migrationManager/childUsage"
	shared "dataManager/shared"
	"fmt"
	"sync"
)

// MigrationManagerDecisionState
// - Decision state -> in this state the Migration manager ignore all the messages, take decisions on collection migration; consume the stack
type MigrationManagerDecisionState struct {
	policy *ErmesStrongCollectionPolicy

	requestStack *stack.CallStack
}
type Decision struct {
	Destination  string
	SendAsLeader bool
}

// methods
func (m *MigrationManagerDecisionState) Setup(args ...any) {

	fmt.Printf("DEFAULT-STRONG-POLICY, switch to DECISION state\n")
	m.requestStack = stack.NewCallStack()

	m.computeEqState()

	m.setLruToSafeLevel()

	//back in idle state
	m.policy.transitionToIdle()

	go m.consumeStrongRequests()
}

// ManageNewStrongRequest save strong request in a buffer for future consumption
func (m *MigrationManagerDecisionState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//sync -> all op in MigrationManagerDecisionState are sync => lock == wait until all decision are taken
	m.requestStack.Push(func() { m.policy.idleState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency) })
}

// after taking all the decisions strong request can be consumed => in the next idle round
func (m *MigrationManagerDecisionState) consumeStrongRequests() {
	for m.requestStack.Len() > 0 {
		call := m.requestStack.Pop()
		call()
	}
	fmt.Printf("DEFAULT-STRONG-POLICY, all strong requests consumed\n")
}

func (m *MigrationManagerDecisionState) ManageGossipMessage(msg gossip.GossipMessage) {
	m.policy.log.Warn("DECISION-POLICY, gossip message received in decision state => ignored")
}

func (m *MigrationManagerDecisionState) computeElasticForce(cb *usage.CircularBuffer, data usage.GetUsage) (int, int) {
	var fe = 0
	var reqT = 0

	for i := 0; i < cb.Capacity(); i++ {
		el := cb.Get(i)
		if el == nil {
			break
		}
		resourceUsage := data(el)
		//m.policy.log.Info("DEFAULT-STRONG-POLICY, compute elastic force", "index", i, "resource_usage", fmt.Sprintf("%+v", resourceUsage))
		reqT += m.policy.penalityFunc(i, resourceUsage.TotalRequests)
		fe += m.policy.penalityFunc(i, resourceUsage.GetElasticForce())
	}

	return fe, reqT
}

func (m *MigrationManagerDecisionState) computeEqState() {
	//sync
	m.policy.partialView.Mutex.Lock()
	defer m.policy.partialView.Mutex.Unlock()

	//imp
	var wg sync.WaitGroup

	m.policy.equilibriumState.Range(func(key, value any) bool { // key: string (resource), value: map[string]*usage.CircularBuffer mapping resource - nodeName
		outerKey := key.(string)
		innerMap := value.(map[string]*usage.CircularBuffer)
		//apply decision only on strong local collections
		col := m.policy.partialView.KnownCollections[outerKey]
		if col == nil ||
			!col.IsLocal() ||
			!col.IsStrong() ||
			col.(*shared.StrongCollection).HasPostponedDecisionBehaviour() {
			return true
		}
		wg.Add(1)
		m.policy.log.Infof("DEFAULT-STRONG-POLICY, start taking decisions for collection %s", outerKey)
		//take decision on a secondary thread
		go m.innerKeyDecision(outerKey, innerMap, &wg)
		return true
	})
	//wait for all the decision to ends
	wg.Wait()
}

func (m *MigrationManagerDecisionState) innerKeyDecision(outerKey string, innerMap map[string]*usage.CircularBuffer, wg *sync.WaitGroup) {
	defer wg.Done()

	replicaInterestedNodeCounter := 0
	leaderDecisionTaken := false

	strongCollection := m.policy.partialView.KnownCollections[outerKey].(*shared.StrongCollection)
	decisions := []Decision{}

	//compute total forces sum -> value that must be outclassed to delegate the leader
	var totalForce = 0
	var totalReqNumber = 0
	if strongCollection.IsLeader {
		for _, buffer := range innerMap {
			wForce, wReq := m.computeElasticForce(buffer, usage.GetWrite)
			//m.policy.log.Info("DEFAULT-STRONG-POLICY, leader total force computation", "collection_id", outerKey, "force", wForce, "num_requests", wReq)
			totalForce += wForce
			totalReqNumber += wReq

			_, rReq := m.computeElasticForce(buffer, usage.GetRead)
			if rReq > 0 {
				//the node is interested to a replica
				replicaInterestedNodeCounter++
			}
		}
		totalForce += m.policy.currentConfig.LeaderThreshold
		m.policy.log.Info("DEFAULT-STRONG-POLICY, leader total force with threshold", "collection_id", outerKey, "total_force", totalForce, "total_requests", totalReqNumber)
	} else {
		//if the node is not leader all requests are in read mode => all nodes in the inner map are interested to a replica
		replicaInterestedNodeCounter = len(innerMap)
	}

	for innerKey, buffer := range innerMap {
		if innerKey == "user" {
			continue
		}

		if strongCollection.IsLeader && !leaderDecisionTaken {
			//if the collection has been moved and removed locally
			m.policy.log.Info("DEFAULT-STRONG-POLICY, leader taking decision for collection", "collection_id", outerKey, "potential_new_leader", innerKey)
			if m.leaderDecision(outerKey, innerKey, buffer, totalForce, totalReqNumber) {
				leaderDecisionTaken = true
				decisions = append(decisions, Decision{
					Destination:  innerKey,
					SendAsLeader: true,
				})

				continue
			}
		}

		//if present on that host
		if _, ok := m.policy.partialView.KnownCollections[outerKey].HostsId()[innerKey]; ok {
			continue
		}

		if m.replicaDecision(outerKey, innerKey, buffer) {
			replicaInterestedNodeCounter--
			decisions = append(decisions, Decision{
				Destination:  innerKey,
				SendAsLeader: false,
			})
		}

		//after taking all the decision skip to next round for this buffer
		buffer.MoveHead()
	}

	if leaderDecisionTaken {
		for _, buffer := range innerMap {
			buffer.ResetWrite()
		}
	}

	m.applyTakenDecisions(outerKey, replicaInterestedNodeCounter, decisions, strongCollection, leaderDecisionTaken)

}

// return true if the leader has been moved (only 1 direction can return true)
func (m *MigrationManagerDecisionState) leaderDecision(outerKey string, innerKey string, buffer *usage.CircularBuffer, totalForce int, totalReq int) bool {
	//compute decision
	feW, req := m.computeElasticForce(buffer, usage.GetWrite)

	latency := m.policy.serverInfo.GetLatencyFor(innerKey)

	m.policy.log.Info("DEFAULT-STRONG-POLICY, leader latency retrieval", "collection_id", outerKey, "potential_new_leader", innerKey, "NodeResourceUsage", fmt.Sprintf("%+v", m.policy.partialView.NodeResourceUsage))
	m.policy.log.Info("DEFAULT-STRONG-POLICY, leader decision computation", "collection_id", outerKey, "elastic_force", feW, "num_requests", req, "latency", latency, "free_memory", m.policy.partialView.NodeResourceUsage[innerKey].FreeRedisMemory, "gravity_constant", m.policy.currentConfig.GravityConstant, "total_requests", totalReq, "total_force", totalForce)
	var driverForce = float64(2*feW) - ((1 / m.policy.partialView.NodeResourceUsage[innerKey].FreeRedisMemory) * m.policy.currentConfig.GravityConstant) - float64(totalReq-req)*float64(latency) - float64(totalForce)
	m.policy.log.Infof("DEFAULT-STRONG-POLICY, leader decision on collection %s result in force %f and resistance %f", outerKey, driverForce, float64(totalForce))
	//evaluate decision
	if driverForce < 0 {
		m.policy.log.Infof("DEFAULT-STRONG-POLICY, leader decision on collection %s has negative reseult for node %s", outerKey, innerKey)
		return false
	}
	m.policy.log.Infof("DEFAULT-STRONG-POLICY, leader decision on collection %s has positive result for node %s", outerKey, innerKey)

	buffer.ResetRead()

	return true
}

func (m *MigrationManagerDecisionState) replicaDecision(outerKey string, innerKey string, buffer *usage.CircularBuffer) bool {
	//compute decision
	feR, _ := m.computeElasticForce(buffer, usage.GetRead)
	resistance := ((1 / m.policy.partialView.NodeResourceUsage[innerKey].FreeRedisMemory) * m.policy.currentConfig.GravityConstant) + float64(m.policy.currentConfig.FollowerThreshold)
	m.policy.log.Infof("DEFAULT-STRONG-POLICY, follower decision on collection %s result in force %d and resistance %f", outerKey, feR, resistance)

	//evaluate decision
	if float64(feR) <= resistance {
		m.policy.log.Infof("DEFAULT-STRONG-POLICY, follower decision on collection %s has negative result for node %s", outerKey, innerKey)
		return false
	}
	m.policy.log.Infof("DEFAULT-STRONG-POLICY, follower decision on collection %s has positive result for node %s", outerKey, innerKey)

	buffer.ResetRead()
	return true
}

func (m *MigrationManagerDecisionState) applyTakenDecisions(outerKey string, replicaInterestedNodeCounter int, decisions []Decision, strongCollection *shared.StrongCollection, leaderDecisionTaken bool) error {
	isMigration := (replicaInterestedNodeCounter == 0 && !strongCollection.IsLeader) || (strongCollection.IsLeader && replicaInterestedNodeCounter == 0 && leaderDecisionTaken)
	m.policy.log.Info("DEFAULT-STRONG-POLICY, apply taken decisions", "collection_id", outerKey, "num_decisions", len(decisions), "is_migration", isMigration, "leader_decision_taken", leaderDecisionTaken, "strong_collection_is_leader", strongCollection.IsLeader, "interested_node_counter", replicaInterestedNodeCounter)

	//sync -> avoid send a collection if used
	localCollection := strongCollection.GetCollectionState().(*shared.LocalCollection)
	lockAcquired := true

	if isMigration || leaderDecisionTaken {
		lockAcquired = localCollection.CollectionLock.TryLock()
		if !lockAcquired {
			m.policy.log.Warn("DEFAULT-STRONG-POLICY, cannot apply taken decisions because collection is in use", "collection_id", outerKey)
		} else {
			defer localCollection.CollectionLock.Unlock()
		}
	}

	//get collection from redis instance
	res, groups, owner, groupsSequence, err := m.policy.dbInterface.GetCollection(outerKey)
	if err != nil {
		m.policy.log.Error("Error getting collection from database", "error", err)
		return err
	}
	collectionSize, sizeError := m.policy.dbInterface.GetCollectionMemoryUsage(outerKey)
	if sizeError != nil {
		m.policy.log.Error("Error getting collection size from database", "error", sizeError)
		return sizeError
	}

	//send collection to the remote nodes that has positive decision
	for _, decision := range decisions {
		if leaderDecisionTaken && decision.SendAsLeader && !lockAcquired {
			m.policy.log.Warn("DEFAULT-STRONG-POLICY, cannot send collection as leader because collection is in use", "collection_id", outerKey, "destination", decision.Destination)
			//postpone the send as leader behaviour to when the lock will be released
			strongCollection.AddPostponedSendBehaviour(func() {
				m.policy.log.Info("DEFAULT-STRONG-POLICY, apply postponed leader decision for collection", "collectionId", strongCollection.CollectionId(), "to", decision.Destination, "as leader:", decision.SendAsLeader)
				strongCollection.GetCollectionState().AddCollectionMigrationHost(decision.Destination)
				m.policy.sender.SendRemoteCollectionTo(decision.Destination, strongCollection, decision.SendAsLeader, res, groups, owner, groupsSequence, collectionSize, isMigration)
				strongCollection.IsLeader = false
				strongCollection.LeaderId = decision.Destination
			})
			leaderDecisionTaken = false
			continue
		}
		m.policy.log.Info("send collection", "collectionId", strongCollection.CollectionId(), "to", decision.Destination, "as leader:", decision.SendAsLeader)
		if leaderDecisionTaken && decision.SendAsLeader {
			strongCollection.IsLeader = false
			strongCollection.LeaderId = decision.Destination
		}

		strongCollection.GetCollectionState().AddCollectionMigrationHost(decision.Destination)
		m.policy.sender.SendRemoteCollectionTo(decision.Destination, strongCollection, decision.SendAsLeader, res, groups, owner, groupsSequence, collectionSize, isMigration)
	}

	if !isMigration {
		m.tryToExecutePostponed(strongCollection)
		return nil
	} else if !lockAcquired {
		m.policy.log.Warn("DEFAULT-STRONG-POLICY, cannot delete collection because collection is in use", "collection_id", outerKey)
		//postpone the delete behaviour to when the lock will be released
		strongCollection.AddPostponedDeleteBehaviour(func() {
			m.policy.log.Info("DEFAULT-STRONG-POLICY, apply postponed deletion decision for collection", "collectionId", strongCollection.CollectionId())
			m.deleteCollectionAfterDecisionTaken(strongCollection, outerKey)
		})
		m.tryToExecutePostponed(strongCollection)
		return nil
	}

	m.deleteCollectionAfterDecisionTaken(outerKey, strongCollection, outerKey)
	return nil
}

func (m *MigrationManagerDecisionState) tryToExecutePostponed(strongCollection *shared.StrongCollection) {
	m.policy.log.Info("DEFAULT-STRONG-POLICY, check postponed decision behaviour for collection for safety reason", "collection_id", strongCollection.CollectionId())
	if strongCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.TryLock() {
		m.policy.log.Info("DEFAULT-STRONG-POLICY, starting postponed decision behaviour execution for strong collection for collection for safety reason", "collection_id", strongCollection.CollectionId())
		localColl := strongCollection.GetCollectionState().(*shared.LocalCollection)
		strongCollection.ExecutePostponedDecisionBehaviour(m.policy.log)
		localColl.CollectionLock.Unlock()
	}

}

func (m *MigrationManagerDecisionState) deleteCollectionAfterDecisionTaken(collectionId string, strongCollection *shared.StrongCollection, outerKey string) {
	//set the collection as remote only in the node partial view -> if no more host removed from partial view after response to send collection to
	strongCollection.SetToRemote()

	//remove collection from redis instance
	m.policy.dbInterface.DeleteCollection(outerKey)

	//remove the collection from the lru
	m.policy.lruCache.Remove(outerKey)

	//stop keep track of access for a remote collection
	m.policy.equilibriumState.Delete(outerKey)
}

func (m *MigrationManagerDecisionState) setLruToSafeLevel() {
	sizeGoal := uint(float32(m.policy.lruConfig.Size) * (1.0 - m.policy.lruConfig.SafeLevel))

	if m.policy.serverInfo.Father == nil {
		if m.policy.lruCache.GetAvailableSize() <= sizeGoal {
			m.policy.log.Error("DEFAULT-STRONG-POLICY, cannot achieve safe level on lru cache because there is no father node to send collections to", "lru_size", m.policy.lruCache.GetCurrentSize(), "size_goal", sizeGoal)
		}
		return
	}

	//sync
	m.policy.partialView.Mutex.Lock()
	defer m.policy.partialView.Mutex.Unlock()

	m.policy.log.InfofWith("DEFAULT-STRONG-POLICY, set lru to safe level %d", []any{sizeGoal}, "lru_size", m.policy.lruCache.GetCurrentSize())
	collectionList := m.policy.lruCache.PeekFreeToAchieveSize(sizeGoal) //get elements to remove from lru without removing them
	m.policy.log.Info("DEFAULT-STRONG-POLICY, collections to send to remote nodes to achieve safe level", "num_collections", collectionList.Len(), "collections", lru.PrettyPrintList(collectionList), "new_lru_size", m.policy.lruCache.GetCurrentSize())
	var wg sync.WaitGroup

	for item := collectionList.Front(); item != nil; item = item.Next() {
		collection := item.Value.(shared.Collection)
		m.policy.log.Info("DEFAULT-STRONG-POLICY, sending collection to remote node to achieve safe level", "collection_id", collection.CollectionId())
		wg.Add(1)
		go m.sendCollectionAndRemove(collection, &wg)
	}

	wg.Wait()
}

func (m *MigrationManagerDecisionState) sendCollectionAndRemove(collection shared.Collection, wg *sync.WaitGroup) error {
	defer wg.Done()

	//sync -> avoid send a collection if used
	b := collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.TryLock()
	if !b {
		return nil
	}
	defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()

	//remove collection from lru
	m.policy.lruCache.Remove(collection.CollectionId())

	//get collection from redis instance
	res, groups, owner, groupsSequence, err := m.policy.dbInterface.GetCollection(collection.CollectionId())
	if err != nil {
		m.policy.log.Error("Error getting collection from database", "error", err)
		return err //TODO: decide
	}
	collectionSize, sizeError := m.policy.dbInterface.GetCollectionMemoryUsage(collection.CollectionId())
	if sizeError != nil {
		m.policy.log.Error("Error getting collection size from database", "error", sizeError)
		return sizeError //TODO: decide
	}

	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	isLeader := false
	if collection.IsStrong() {
		isLeader = collection.(*shared.StrongCollection).IsLeader
		if isLeader {
			collection.(*shared.StrongCollection).LeaderId = m.policy.serverInfo.Father.HostName
		}
		//stop keep track of access for a remote collection
		m.policy.RemoveInfo(collection.CollectionId())
	}

	fatherName := m.policy.serverInfo.Father.HostName
	collection.GetCollectionState().AddCollectionMigrationHost(fatherName)
	m.policy.sender.SendRemoteCollectionTo(fatherName, collection, isLeader, res, groups, owner, groupsSequence, collectionSize, true)

	//remove collection from redis instance
	m.policy.dbInterface.DeleteCollection(collection.CollectionId())

	//set the collection as remote only in the node partial view
	collection.SetToRemote()

	return nil
}
