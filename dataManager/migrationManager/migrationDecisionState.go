package migrationManager

import gossipmessages "dataManager/migrationManager/gossipMessages"

// - Decision state -> in this state the Migration manager ingore all the messages, take decisions on collection migration; consume the stack
type MigrationManagerDecisionState struct {
	MigrationManagerImp *MigrationManagerImp
}

// methods

func (s *MigrationManagerDecisionState) Setup(args ...any) {}

func (m *MigrationManagerDecisionState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//TODO
}

func (m *MigrationManagerDecisionState) ManageGossipMessage(msg gossipmessages.GossipMessage) {
	//TODO -> ignore messages
}

func (m *MigrationManagerDecisionState) computeElasticForce(cb *CircularBuffer, data getUsage) int {
	var feR = 0

	for i := 0; i < cb.capacity; i++ {
		idx := (cb.Head - i + cb.capacity) % cb.capacity
		if cb.Data[idx] == nil {
			break
		}
		feR += m.MigrationManagerImp.penalityFunc(i, GetElasticForce(data(cb, idx)))
	}

	return feR
}

type getUsage func(*CircularBuffer, int) resourceUsage

func getRead(cb *CircularBuffer, idx int) resourceUsage {
	return cb.Data[idx].read
}
func getWrite(cb *CircularBuffer, idx int) resourceUsage {
	return cb.Data[idx].write
}
