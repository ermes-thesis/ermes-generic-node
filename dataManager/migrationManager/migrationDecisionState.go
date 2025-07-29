package migrationManager

import (
	gossip "dataManager/migrationManager/gossipMessages"
	shared "dataManager/shared"
	"sync"
)

// - Decision state -> in this state the Migration manager ingore all the messages, take decisions on collection migration; consume the stack
type MigrationManagerDecisionState struct {
	MigrationManagerImp *MigrationManagerImp
	childResourceUsage  map[string]childStateInfo
}

// methods
func (m *MigrationManagerDecisionState) Setup(args ...any) {
	if len(args) != 1 {
		panic("invalid arguments for Setup in DecisionState")
	}
	childrenResourceUsage, ok := args[0].(map[string]childStateInfo)
	if !ok {
		panic("expected []childStateInfo for childrenResourceUsage")
	}
	m.childResourceUsage = childrenResourceUsage

	m.computeEqState()

}

// TODO: valutare se ha senso inserire in stack senza lock e poi parsare stack dopo
func (m *MigrationManagerDecisionState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//sync -> all op in MigrationManagerDecisionState are sync => lock == wait until all decsion are taken
	lock.Lock()
	defer lock.Unlock()

	//imp
	m.MigrationManagerImp.idleState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency)
}

func (m *MigrationManagerDecisionState) ManageGossipMessage(msg gossip.GossipMessage) {
	//TODO -> ignore messages
}

func (m *MigrationManagerDecisionState) computeElasticForce(cb *CircularBuffer, data getUsage) (int, int) {
	var fe = 0
	var reqT = 0

	for i := 0; i < cb.capacity; i++ {
		idx := (cb.Head - i + cb.capacity) % cb.capacity
		if cb.Data[idx] == nil {
			break
		}
		reqT += data(cb, idx).TotalRequests //TODO: penality anche per questo?
		fe += m.MigrationManagerImp.penalityFunc(i, GetElasticForce(data(cb, idx)))
	}

	return fe, reqT
}

type getUsage func(*CircularBuffer, int) resourceUsage

func getRead(cb *CircularBuffer, idx int) resourceUsage {
	return cb.Data[idx].read
}
func getWrite(cb *CircularBuffer, idx int) resourceUsage {
	return cb.Data[idx].write
}

func (m *MigrationManagerDecisionState) computeEqState() {
	//sync
	m.MigrationManagerImp.collectionsPartialView.Mutex.Lock()
	defer m.MigrationManagerImp.collectionsPartialView.Mutex.Unlock()

	//imp
	var wg sync.WaitGroup
	wg.Add(len(m.MigrationManagerImp.unstableEquilibriumState))

	for outerKey, innerMap := range m.MigrationManagerImp.unstableEquilibriumState {
		//apply decision only on strong local collections
		if m.MigrationManagerImp.collectionsPartialView.KnowedCollections[outerKey] == nil ||
			!m.MigrationManagerImp.collectionsPartialView.KnowedCollections[outerKey].IsLocal() ||
			!m.MigrationManagerImp.collectionsPartialView.KnowedCollections[outerKey].IsStrong() {
			continue
		}

		//take decision on a secondary thread
		go m.innerKeyDecision(outerKey, innerMap, &wg, len(innerMap))

	}

	//wait for all the decision to ends
	wg.Wait()
}

func (m *MigrationManagerDecisionState) innerKeyDecision(outerKey string, innerMap map[string]*CircularBuffer, wg *sync.WaitGroup, interestedNodeCounter int) {
	defer wg.Done()

	strongCollection := m.MigrationManagerImp.collectionsPartialView.KnowedCollections[outerKey].(*shared.StrongCollection)

	//compute total forces sum -> value that must be outclassed to delegate the leader
	var totalForce = 0
	var totalReqNumber = 0
	if strongCollection.IsLeader {
		for _, buffer := range innerMap {
			force, req := m.computeElasticForce(buffer, getWrite)
			totalForce += force
			totalReqNumber += req
		}
		totalForce += m.MigrationManagerImp.config.LeaderThreshold
	}

	for innerKey, buffer := range innerMap {
		if strongCollection.IsLeader {
			//if the collection has been moved and removed locally
			if m.leaderDecision(outerKey, innerKey, buffer, strongCollection, totalForce, totalReqNumber) {
				break
			}
		}

		m.replicaDecision(outerKey, innerKey, buffer, strongCollection, interestedNodeCounter)

		//after taking all the decision skip to next round for this buffer
		buffer.Movehead()

	}
}

// return true if the leader has been moved (only 1 direction can return true)
func (m *MigrationManagerDecisionState) leaderDecision(outerKey string, innerKey string, buffer *CircularBuffer, strongCollection *shared.StrongCollection, totalForce int, totalReq int) bool {
	//compute decision
	feW, _ := m.computeElasticForce(buffer, getWrite)
	var driverForce = float64(2*feW) - (m.childResourceUsage[innerKey].MemoryUsage * m.MigrationManagerImp.config.GravityConstant) - float64(totalReq) //TODO: moltiplicaremper distanza geografica

	//evaluate decision
	if driverForce <= float64(totalForce) {
		return false
	}

	//react to positive decision
	//get collection from redis instance
	res, groups, err := m.MigrationManagerImp.dbInterface.GetCollection(outerKey)
	if err != nil {
		panic(err) //TODO: decide
	}
	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	m.MigrationManagerImp.sendRemoteCollectionTo(innerKey, strongCollection, res, groups)

	//remove collection from redis instance
	m.MigrationManagerImp.dbInterface.DeleteCollection(outerKey)

	//set the collection as remote only in the node partial view
	strongCollection.SetToRemote()

	return true
}

func (m *MigrationManagerDecisionState) replicaDecision(outerKey string, innerKey string, buffer *CircularBuffer, strongCollection *shared.StrongCollection, interestedNodeCounter int) bool {
	//compute decision
	feR, _ := m.computeElasticForce(buffer, getRead)
	resistance := (m.childResourceUsage[innerKey].MemoryUsage * m.MigrationManagerImp.config.GravityConstant) + float64(m.MigrationManagerImp.config.FollowerThreshold)

	//evaluate decision
	if float64(feR) <= resistance {
		return false
	}

	//react to positive decision
	//get collection from redis instance
	res, groups, err := m.MigrationManagerImp.dbInterface.GetCollection(outerKey)
	if err != nil {
		panic(err) //TODO: decide
	}
	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	m.MigrationManagerImp.sendRemoteCollectionTo(innerKey, strongCollection, res, groups)

	//if all the interest adjacent node has been satisfied
	if interestedNodeCounter-len(strongCollection.HostsId()) <= 0 {
		//remove collection from redis instance
		m.MigrationManagerImp.dbInterface.DeleteCollection(outerKey)

		//set the collection as remote only in the node partial view
		strongCollection.SetToRemote()
	}

	return true
}
