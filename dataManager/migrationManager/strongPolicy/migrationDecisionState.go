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
	policy            *ErmesStrongCollectionPolicy
	NodeResourceUsage map[string]NodeStateInfo

	requestStack stack.CallStack
}

// methods
func (m *MigrationManagerDecisionState) Setup(args ...any) {
	if len(args) != 1 {
		panic("invalid arguments for Setup in DecisionState")
	}
	childrenResourceUsage, ok := args[0].(map[string]NodeStateInfo)
	if !ok {
		panic("expected []ChildStateInfo for childrenResourceUsage")
	}

	fmt.Printf("DEFAULT-STRONG-POLICY, switch to DECISION state\n")
	m.requestStack = stack.NewCallStack()

	m.NodeResourceUsage = childrenResourceUsage

	m.computeEqState()

	m.setLruToSafeLevel()

	go m.consumeStrongRequests()

	//back in idle state
	m.policy.transitionToIdle()
}

// ManageNewStrongRequest save strong request in a buffer for future consumption
func (m *MigrationManagerDecisionState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//sync -> all op in MigrationManagerDecisionState are sync => lock == wait until all decision are taken
	m.requestStack.Push(func() { m.policy.idleState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency) })
}

// after taking all the decisions strong request can be consumed => in the next idle round
func (m *MigrationManagerDecisionState) consumeStrongRequests() {
	for len(m.requestStack) > 0 {
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
		reqT += resourceUsage.TotalRequests //TODO: penality anche per questo?
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
	wg.Add(m.policy.equilibriumState.Count())

	m.policy.equilibriumState.Range(func(key, value any) bool {
		outerKey := key.(string)
		innerMap := value.(map[string]*usage.CircularBuffer)
		//apply decision only on strong local collections
		if m.policy.partialView.KnownCollections[outerKey] == nil ||
			!m.policy.partialView.KnownCollections[outerKey].IsLocal() ||
			!m.policy.partialView.KnownCollections[outerKey].IsStrong() {
			wg.Done()
			return true
		}

		m.policy.log.Infof("DEFAULT-STRONG-POLICY, start taking decisions for collection %s", outerKey)
		//take decision on a secondary thread
		go m.innerKeyDecision(outerKey, innerMap, &wg, len(innerMap))
		return true
	})
	//wait for all the decision to ends
	wg.Wait()
}

func (m *MigrationManagerDecisionState) innerKeyDecision(outerKey string, innerMap map[string]*usage.CircularBuffer, wg *sync.WaitGroup, interestedNodeCounter int) {
	defer wg.Done()

	strongCollection := m.policy.partialView.KnownCollections[outerKey].(*shared.StrongCollection)

	//compute total forces sum -> value that must be outclassed to delegate the leader
	var totalForce = 0
	var totalReqNumber = 0
	if strongCollection.IsLeader {
		for _, buffer := range innerMap {
			force, req := m.computeElasticForce(buffer, usage.GetWrite)
			totalForce += force
			totalReqNumber += req
		}
		totalForce += m.policy.currentConfig.LeaderThreshold
	}

	for innerKey, buffer := range innerMap {
		if innerKey == "user" {
			continue
		}
		//if present on that host
		if _, ok := m.policy.partialView.KnownCollections[outerKey].HostsId()[innerKey]; ok {
			continue
		}

		if strongCollection.IsLeader {
			//if the collection has been moved and removed locally
			if m.leaderDecision(outerKey, innerKey, buffer, strongCollection, totalForce, totalReqNumber) {
				return
			}
		}

		m.replicaDecision(outerKey, innerKey, buffer, strongCollection, interestedNodeCounter)

		//after taking all the decision skip to next round for this buffer
		buffer.MoveHead()

	}
}

// return true if the leader has been moved (only 1 direction can return true)
func (m *MigrationManagerDecisionState) leaderDecision(outerKey string, innerKey string, buffer *usage.CircularBuffer, strongCollection *shared.StrongCollection, totalForce int, totalReq int) bool {
	//compute decision
	feW, req := m.computeElasticForce(buffer, usage.GetWrite)

	var latency int
	for _, child := range m.policy.serverInfo.Children {
		if child.HostName == innerKey {
			latency = child.Latency
			break
		}
	}

	var driverForce = float64(2*feW) - ((1 / m.NodeResourceUsage[innerKey].FreeRedisMemory) * m.policy.currentConfig.GravityConstant) - float64(totalReq-req)*float64(latency)
	m.policy.log.Infof("DEFAULT-STRONG-POLICY, leader decision on collection %s result in force %f and resistance %f", outerKey, driverForce, float64(totalForce))
	//evaluate decision
	if driverForce <= float64(totalForce) {
		m.policy.log.Infof("DEFAULT-STRONG-POLICY, leader decision on collection %s has negative reseult for node %s", outerKey, innerKey)
		return false
	}
	m.policy.log.Infof("DEFAULT-STRONG-POLICY, leader decision on collection %s has positive result for node %s", outerKey, innerKey)

	//react to positive decision
	//sync -> avoid send a collection if used
	strongCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Lock()
	defer strongCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()

	//get collection from redis instance
	res, groups, owner, groupsSequence, err := m.policy.dbInterface.GetCollection(outerKey)
	if err != nil {
		panic(err) //TODO: decide
	}
	collectionSize, sizeError := m.policy.dbInterface.GetCollectionMemoryUsage(outerKey)
	if sizeError != nil {
		panic(sizeError) //TODO: decide
	}
	//s
	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	m.policy.sender.SendRemoteCollectionTo(innerKey, strongCollection, true, res, groups, owner, groupsSequence, collectionSize)

	//remove collection from redis instance
	m.policy.dbInterface.DeleteCollection(outerKey)

	//set the collection as remote only in the node partial view
	strongCollection.SetToRemote()
	strongCollection.LeaderId = innerKey

	//remove the collection from the lru
	m.policy.lruCache.Remove(outerKey)

	//stop keep track of access for a remote collection
	m.policy.equilibriumState.Delete(outerKey)
	return true
}

func (m *MigrationManagerDecisionState) replicaDecision(outerKey string, innerKey string, buffer *usage.CircularBuffer, strongCollection *shared.StrongCollection, interestedNodeCounter int) bool {
	//compute decision
	feR, _ := m.computeElasticForce(buffer, usage.GetRead)
	resistance := ((1 / m.NodeResourceUsage[innerKey].FreeRedisMemory) * m.policy.currentConfig.GravityConstant) + float64(m.policy.currentConfig.FollowerThreshold)
	m.policy.log.Infof("DEFAULT-STRONG-POLICY, follower decision on collection %s result in force %d and resistance %f", outerKey, feR, resistance)

	//evaluate decision
	if float64(feR) <= resistance {
		m.policy.log.Infof("DEFAULT-STRONG-POLICY, follower decision on collection %s has negative result for node %s", outerKey, innerKey)
		return false
	}
	m.policy.log.Infof("DEFAULT-STRONG-POLICY, follower decision on collection %s has positive result for node %s", outerKey, innerKey)

	//react to positive decision
	//sync -> avoid send a collection if used
	strongCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Lock()
	defer strongCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()

	//get collection from redis instance
	res, groups, owner, groupsSequence, err := m.policy.dbInterface.GetCollection(outerKey)
	if err != nil {
		panic(err) //TODO: decide
	}
	collectionSize, sizeError := m.policy.dbInterface.GetCollectionMemoryUsage(outerKey)
	if sizeError != nil {
		panic(sizeError) //TODO: decide
	}
	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	m.policy.sender.SendRemoteCollectionTo(innerKey, strongCollection, false, res, groups, owner, groupsSequence, collectionSize)

	//if all the interest adjacent node has been satisfied
	if interestedNodeCounter-len(strongCollection.HostsId()) <= 0 && !strongCollection.IsLeader {
		//remove collection from redis instance
		m.policy.dbInterface.DeleteCollection(outerKey)

		//set the collection as remote only in the node partial view
		strongCollection.SetToRemote()

		//remove the collection from the lru
		m.policy.lruCache.Remove(outerKey)

		//stop keep track of access for a remote collection
		m.policy.equilibriumState.Delete(outerKey)
	}

	return true
}

func (m *MigrationManagerDecisionState) setLruToSafeLevel() {
	sizeGoal := uint(float32(m.policy.lruConfig.Size) * (1.0 - m.policy.lruConfig.SafeLevel))
	m.policy.log.Infof("DEFAULT-STRONG-POLICY, set lru to safe level %d", sizeGoal)
	collectionList := m.policy.lruCache.FreeToAchieveSize(sizeGoal)
	var wg sync.WaitGroup
	wg.Add(collectionList.Len())

	for item := collectionList.Front(); item != nil; item = item.Next() {
		collection := item.Value.(shared.Collection)
		go m.sendCollectionAndRemove(collection, &wg)
	}

	wg.Wait()
}

func (m *MigrationManagerDecisionState) sendCollectionAndRemove(collection shared.Collection, wg *sync.WaitGroup) {
	defer wg.Done()

	//sync -> avoid send a collection if used
	collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Lock()
	defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()

	//get collection from redis instance
	res, groups, owner, groupsSequence, err := m.policy.dbInterface.GetCollection(collection.CollectionId())
	if err != nil {
		panic(err) //TODO: decide
	}
	collectionSize, sizeError := m.policy.dbInterface.GetCollectionMemoryUsage(collection.CollectionId())
	if sizeError != nil {
		panic(sizeError) //TODO: decide
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
	m.policy.sender.SendRemoteCollectionTo(m.policy.serverInfo.Father.HostName, collection, isLeader, res, groups, owner, groupsSequence, collectionSize)

	//remove collection from redis instance
	m.policy.dbInterface.DeleteCollection(collection.CollectionId())

	//set the collection as remote only in the node partial view
	collection.SetToRemote()
}
