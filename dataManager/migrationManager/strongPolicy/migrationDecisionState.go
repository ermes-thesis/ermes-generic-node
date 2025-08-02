package strongPolicy

import (
	stack "dataManager/migrationManager/callbackStack"
	usage "dataManager/migrationManager/childUsage"
	gossip "dataManager/migrationManager/gossipMessages"
	shared "dataManager/shared"
	"logger"
	"sync"
)

// - Decision state -> in this state the Migration manager ingore all the messages, take decisions on collection migration; consume the stack
type MigrationManagerDecisionState struct {
	policy            *DefaultStrongCollectionPolicy
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
	m.requestStack = stack.NewCallStack()

	m.NodeResourceUsage = childrenResourceUsage

	m.computeEqState()

	go m.consumeStrongRequests()

	//back in idel state
	m.policy.transitionToIdle()
}

// save strong request in a buffer for future consumption
func (m *MigrationManagerDecisionState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//sync -> all op in MigrationManagerDecisionState are sync => lock == wait until all decsion are taken
	m.requestStack.Push(func() { m.policy.idleState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency) })
}

// after taking all the decisions strong request can be consumed => in the next idle round
func (m *MigrationManagerDecisionState) consumeStrongRequests() {
	for len(m.requestStack) > 0 {
		call := m.requestStack.Pop()
		call()
	}
}

func (m *MigrationManagerDecisionState) ManageGossipMessage(msg gossip.GossipMessage) {
	m.policy.logger.Println("DECISION-POLICY", "gossip message received in decision state => ignored", logger.WARNING)
}

func (m *MigrationManagerDecisionState) computeElasticForce(cb *usage.CircularBuffer, data usage.GetUsage) (int, int) {
	var fe = 0
	var reqT = 0

	for i := 0; i < cb.Capacity; i++ {
		idx := (cb.Head - i + cb.Capacity) % cb.Capacity
		if cb.Data[idx] == nil {
			break
		}
		reqT += data(cb, idx).TotalRequests //TODO: penality anche per questo?
		fe += m.policy.penalityFunc(i, usage.GetElasticForce(data(cb, idx)))
	}

	return fe, reqT
}

func (m *MigrationManagerDecisionState) computeEqState() {
	//sync
	m.policy.partialView.Mutex.Lock()
	defer m.policy.partialView.Mutex.Unlock()

	//imp
	var wg sync.WaitGroup
	wg.Add(len(m.policy.equilibriumState))

	for outerKey, innerMap := range m.policy.equilibriumState {
		//apply decision only on strong local collections
		if m.policy.partialView.KnowedCollections[outerKey] == nil ||
			!m.policy.partialView.KnowedCollections[outerKey].IsLocal() ||
			!m.policy.partialView.KnowedCollections[outerKey].IsStrong() {
			continue
		}

		//take decision on a secondary thread
		go m.innerKeyDecision(outerKey, innerMap, &wg, len(innerMap))

	}

	//wait for all the decision to ends
	wg.Wait()
}

func (m *MigrationManagerDecisionState) innerKeyDecision(outerKey string, innerMap map[string]*usage.CircularBuffer, wg *sync.WaitGroup, interestedNodeCounter int) {
	defer wg.Done()

	strongCollection := m.policy.partialView.KnowedCollections[outerKey].(*shared.StrongCollection)

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
		if strongCollection.IsLeader {
			//if the collection has been moved and removed locally
			if m.leaderDecision(outerKey, innerKey, buffer, strongCollection, totalForce, totalReqNumber) {
				return
			}
		}

		m.replicaDecision(outerKey, innerKey, buffer, strongCollection, interestedNodeCounter)

		//after taking all the decision skip to next round for this buffer
		buffer.Movehead()

	}
}

// return true if the leader has been moved (only 1 direction can return true)
func (m *MigrationManagerDecisionState) leaderDecision(outerKey string, innerKey string, buffer *usage.CircularBuffer, strongCollection *shared.StrongCollection, totalForce int, totalReq int) bool {
	//compute decision
	feW, req := m.computeElasticForce(buffer, usage.GetWrite)
	var driverForce = float64(2*feW) - (m.NodeResourceUsage[innerKey].FreeRedisMemory * m.policy.currentConfig.GravityConstant) - float64(totalReq-req)*float64(m.policy.serverInfo.Latency)

	//evaluate decision
	if driverForce <= float64(totalForce) {
		return false
	}

	//react to positive decision
	//get collection from redis instance
	res, groups, err := m.policy.dbInterface.GetCollection(outerKey)
	if err != nil {
		panic(err) //TODO: decide
	}
	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	m.policy.sender.SendRemoteCollectionTo(innerKey, strongCollection, true, res, groups)

	//remove collection from redis instance
	m.policy.dbInterface.DeleteCollection(outerKey)

	//set the collection as remote only in the node partial view
	strongCollection.SetToRemote()
	strongCollection.LeaderId = innerKey

	//remove the collection from the lru
	m.policy.lruCache.Remove(outerKey)

	//stop keep track of access for a remote collection
	delete(m.policy.equilibriumState, outerKey)

	return true
}

func (m *MigrationManagerDecisionState) replicaDecision(outerKey string, innerKey string, buffer *usage.CircularBuffer, strongCollection *shared.StrongCollection, interestedNodeCounter int) bool {
	//compute decision
	feR, _ := m.computeElasticForce(buffer, usage.GetRead)
	resistance := (m.NodeResourceUsage[innerKey].FreeRedisMemory * m.policy.currentConfig.GravityConstant) + float64(m.policy.currentConfig.FollowerThreshold)

	//evaluate decision
	if float64(feR) <= resistance {
		return false
	}

	//react to positive decision
	//get collection from redis instance
	res, groups, err := m.policy.dbInterface.GetCollection(outerKey)
	if err != nil {
		panic(err) //TODO: decide
	}
	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	m.policy.sender.SendRemoteCollectionTo(innerKey, strongCollection, false, res, groups)

	//if all the interest adjacent node has been satisfied
	if interestedNodeCounter-len(strongCollection.HostsId()) <= 0 && !strongCollection.IsLeader {
		//remove collection from redis instance
		m.policy.dbInterface.DeleteCollection(outerKey)

		//set the collection as remote only in the node partial view
		strongCollection.SetToRemote()

		//remove the collection from the lru
		m.policy.lruCache.Remove(outerKey)

		//stop keep track of access for a remote collection
		delete(m.policy.equilibriumState, outerKey)
	}

	return true
}
