package strongPolicy

import (
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

type Decisions struct {
	Decisions           []Decision
	CollectionId        string
	IsMigration         bool
	LeaderDecisionTaken bool
	CollectionSize      uint64
}
type Decision struct {
	Destination  string
	SendAsLeader bool
}

// methods
func (m *MigrationManagerDecisionState) Setup(args ...any) {

	m.policy.log.Info("DEFAULT-STRONG-POLICY, switch to DECISION state")
	m.requestStack = stack.NewCallStack()

	m.policy.partialView.Mutex.Lock()

	m.computeEqState()

	//m.setLruToSafeLevel()

	m.policy.partialView.Mutex.Unlock()

	//back in idle state
	m.policy.transitionToIdle()

	go m.consumeStrongRequests()

	m.policy.log.Info("Completed decision state")
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
	m.policy.log.Debug("DEFAULT-STRONG-POLICY, all strong requests consumed\n")
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

	//imp
	var wg sync.WaitGroup

	decisionCh := make(chan Decisions)
	//start decision reader

	go func() {
		//go take decision for each collection in parallel and collect decisions in decisionCh
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
			m.policy.log.Infof("DEFAULT-STRONG-POLICY, start taking decisions for collection %s", outerKey)
			//take decision on a secondary thread
			wg.Go(func() { m.innerKeyDecision(outerKey, innerMap, decisionCh) })
			return true
		})
		//wait for all the decision to ends

		wg.Wait()
		m.policy.log.Info("DEFAULT-STRONG-POLICY, all decision taken, closing decision channel")
		close(decisionCh) //close the decision channel to stop the reader
	}()

	m.decisionReader(decisionCh)
}

func (m *MigrationManagerDecisionState) decisionReader(decisionCh chan Decisions) {
	//collect policy decisions
	aggregatedDecisions := make(map[string]Decisions)
	var freedSize uint64 = 0
	migrationKeys := make(map[string]struct{})
	for decisions := range decisionCh {
		if decisions.IsMigration {
			freedSize += decisions.CollectionSize
			migrationKeys[decisions.CollectionId] = struct{}{}
		}
		for i, d := range decisions.Decisions {
			father := m.policy.serverInfo.Father
			if father != nil && d.Destination == father.HostName {
				// Swap decision to father with the last decision
				decisions.Decisions[i] = decisions.Decisions[len(decisions.Decisions)-1]
				decisions.Decisions[len(decisions.Decisions)-1] = d

				break
			}
		}
		aggregatedDecisions[decisions.CollectionId] = decisions
	}

	//upgrade decisions to reach safe lru level
	m.upgradeDecisionsToReachSafeLRU(aggregatedDecisions, freedSize, migrationKeys)

	wg := sync.WaitGroup{}
	m.policy.log.Warn("DEFAULT-STRONG-POLICY, applying taken decisions")
	for outerKey, decisions := range aggregatedDecisions {
		collection := m.policy.partialView.KnownCollections[outerKey]
		wg.Go(func() {
			m.applyTakenDecisions(outerKey, decisions.IsMigration, decisions.Decisions, collection, decisions.LeaderDecisionTaken)
		})
	}
	wg.Wait()
	m.policy.log.Warn("DEFAULT-STRONG-POLICY, all decisions applied")
}

func (m *MigrationManagerDecisionState) innerKeyDecision(outerKey string, innerMap map[string]*usage.CircularBuffer, decisionCh chan<- Decisions) {
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

	//send taken decisions to the decision reader
	decisionCh <- Decisions{
		Decisions:           decisions,
		CollectionId:        outerKey,
		IsMigration:         (replicaInterestedNodeCounter == 0 && !strongCollection.IsLeader) || (strongCollection.IsLeader && replicaInterestedNodeCounter == 0 && leaderDecisionTaken),
		LeaderDecisionTaken: leaderDecisionTaken,
		CollectionSize:      strongCollection.GetCollectionState().(*shared.LocalCollection).Size(),
	}
	//m.applyTakenDecisions(outerKey, replicaInterestedNodeCounter, decisions, strongCollection, leaderDecisionTaken)

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

func (m *MigrationManagerDecisionState) applyTakenDecisions(outerKey string, isMigration bool, decisions []Decision, collection shared.Collection, leaderDecisionTaken bool) error {
	m.policy.log.Info("DEFAULT-STRONG-POLICY, apply taken decisions", "collection_id", outerKey, "num_decisions", len(decisions), "is_migration", isMigration, "leader_decision_taken", leaderDecisionTaken)

	//sync -> avoid send a collection if used
	localCollection := collection.GetCollectionState().(*shared.LocalCollection)
	lockAcquired := true

	if isMigration || leaderDecisionTaken {
		lockAcquired = localCollection.CollectionLock.TryLock()
		if !lockAcquired {
			m.policy.log.Warn("DEFAULT-STRONG-POLICY, cannot apply taken decisions because collection is in use", "collection_id", outerKey)
			if !collection.IsStrong() {
				return nil
			}
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
		if leaderDecisionTaken && decision.SendAsLeader && !lockAcquired && collection.IsStrong() {
			m.policy.log.Warn("DEFAULT-STRONG-POLICY, cannot send collection as leader because collection is in use", "collection_id", outerKey, "destination", decision.Destination)
			//postpone the send as leader behaviour to when the lock will be released
			collection.(*shared.StrongCollection).AddPostponedSendBehaviour(func() {
				m.policy.log.Info("DEFAULT-STRONG-POLICY, apply postponed leader decision for collection", "collectionId", collection.CollectionId(), "to", decision.Destination, "as leader:", decision.SendAsLeader)
				collection.GetCollectionState().AddCollectionMigrationHost(decision.Destination)
				m.policy.sender.SendRemoteCollectionTo(decision.Destination, collection, decision.SendAsLeader, res, groups, owner, groupsSequence, collectionSize, isMigration)
				collection.(*shared.StrongCollection).IsLeader = false
				collection.(*shared.StrongCollection).LeaderId = decision.Destination
			})
			leaderDecisionTaken = false
			continue
		}
		m.policy.log.Info("send collection", "collectionId", collection.CollectionId(), "to", decision.Destination, "as leader:", decision.SendAsLeader)
		if leaderDecisionTaken && decision.SendAsLeader && collection.IsStrong() {
			collection.(*shared.StrongCollection).IsLeader = false
			collection.(*shared.StrongCollection).LeaderId = decision.Destination
		}

		collection.GetCollectionState().AddCollectionMigrationHost(decision.Destination)
		m.policy.sender.SendRemoteCollectionTo(decision.Destination, collection, decision.SendAsLeader, res, groups, owner, groupsSequence, collectionSize, isMigration)
	}

	if !isMigration && collection.IsStrong() {
		m.tryToExecutePostponed(collection.(*shared.StrongCollection))
		return nil
	} else if !lockAcquired && collection.IsStrong() {
		m.policy.log.Warn("DEFAULT-STRONG-POLICY, cannot delete collection because collection is in use", "collection_id", outerKey)
		//postpone the delete behaviour to when the lock will be released
		collection.(*shared.StrongCollection).AddPostponedDeleteBehaviour(func() {
			m.policy.log.Info("DEFAULT-STRONG-POLICY, apply postponed deletion decision for collection", "collectionId", collection.CollectionId())
			m.deleteCollectionAfterDecisionTaken(collection, outerKey)
		})
		m.tryToExecutePostponed(collection.(*shared.StrongCollection))
		return nil
	}

	m.deleteCollectionAfterDecisionTaken(collection, outerKey)
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

func (m *MigrationManagerDecisionState) deleteCollectionAfterDecisionTaken(collection shared.Collection, outerKey string) {
	//set the collection as remote only in the node partial view -> if no more host removed from partial view after response to send collection to
	collection.SetToRemote()

	//remove collection from redis instance
	m.policy.dbInterface.DeleteCollection(outerKey)

	//remove the collection from the lru
	m.policy.lruCache.Remove(outerKey)

	//stop keep track of access for a remote collection
	m.policy.equilibriumState.Delete(outerKey)
}

func (m *MigrationManagerDecisionState) upgradeDecisionsToReachSafeLRU(aggregatedDecisions map[string]Decisions, freedSize uint64, migrationKeys map[string]struct{}) {
	sizeGoal := uint64(float32(m.policy.lruConfig.Size) * (1.0 - m.policy.lruConfig.SafeLevel))

	available := uint64(m.policy.lruCache.GetAvailableSize()) + freedSize
	if available > sizeGoal {
		return
	}

	father := m.policy.serverInfo.Father
	if father == nil {
		m.policy.log.Error(
			"DEFAULT-STRONG-POLICY, cannot achieve safe level on lru cache because there is no father node",
			"lru_size", m.policy.lruCache.GetCurrentSize(),
			"size_goal", sizeGoal,
		)
		return
	}

	collectionList := m.policy.lruCache.PeekFreeToAchieveSizeWithFreed(sizeGoal, freedSize, migrationKeys)
	for item := collectionList.Front(); item != nil; item = item.Next() {
		collection := item.Value.(shared.Collection)
		id := collection.CollectionId()

		curr, exists := aggregatedDecisions[id]
		if !exists {
			leader := leaderRequired(collection)
			aggregatedDecisions[id] = Decisions{
				CollectionId:        id,
				IsMigration:         true,
				LeaderDecisionTaken: leader,
				CollectionSize:      collection.GetCollectionState().(*shared.LocalCollection).Size(),
				Decisions: []Decision{{
					Destination:  father.HostName,
					SendAsLeader: leader,
				}},
			}
			continue
		}

		aggregatedDecisions[id] = ensureMigrationToFather(
			curr,
			collection,
			father.HostName,
		)
	}
}

func leaderRequired(collection shared.Collection) bool {
	if !collection.IsStrong() {
		return false
	}
	return collection.(*shared.StrongCollection).IsLeader
}

func ensureMigrationToFather(decisions Decisions, collection shared.Collection, father string) Decisions {
	leader := leaderRequired(collection)

	// already migration -> nothing to do
	if decisions.IsMigration {
		return decisions
	}

	decisions.IsMigration = true

	// If last decision destination is towards father enter the if
	lastDecisionPos := len(decisions.Decisions) - 1
	if lastDecisionPos >= 0 && decisions.Decisions[lastDecisionPos].Destination == father {
		if leader && !decisions.LeaderDecisionTaken {
			decisions.Decisions[lastDecisionPos].SendAsLeader = true
			decisions.LeaderDecisionTaken = true
		}
		return decisions
	}

	//for i := range decisions.Decisions {
	//	if decisions.Decisions[i].Destination == father {
	//		if leader && !decisions.LeaderDecisionTaken {
	//			decisions.Decisions[i].SendAsLeader = true
	//			decisions.LeaderDecisionTaken = true
	//		}
	//		return decisions
	//	}
	//}

	// not present -> append
	decisions.Decisions = append(decisions.Decisions, Decision{
		Destination:  father,
		SendAsLeader: leader && !decisions.LeaderDecisionTaken,
	})

	if leader {
		decisions.LeaderDecisionTaken = true
	}

	return decisions
}
