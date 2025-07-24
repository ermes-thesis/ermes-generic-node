//go:build test
// +build test

package migrationManager

func (m *MigrationManagerImp) GetUnstableEquilibriumState() map[string]map[string]*CircularBuffer {
	return m.unstableEquilibriumState
}

func (s *childResourceUsage) GetWrite() resourceUsage {
	return s.write
}

func (s *childResourceUsage) GetRead() resourceUsage {
	return s.read
}

func ResetSingleton() {
	migrationManagareInstance = nil
}

func GetBuffer(collection string, child string) *CircularBuffer {
	return migrationManagareInstance.unstableEquilibriumState[collection][child]
}

func (m *MigrationManagerImp) GetCurrentState() MigrationManagerState {
	return m.currentState
}

func (m *MigrationManagerImp) SetState(s MigrationManagerState) {
	m.setState(s)
}
func (m *MigrationManagerImp) GetDecisionState() MigrationManagerState {
	return m.decisionState
}

func (m *MigrationManagerDecisionState) ComputeReadElasticForce(cb *CircularBuffer) int {
	return m.computeElasticForce(cb, getRead)
}

func (m *MigrationManagerDecisionState) ComputeWriteElasticForce(cb *CircularBuffer) int {
	return m.computeElasticForce(cb, getWrite)
}
