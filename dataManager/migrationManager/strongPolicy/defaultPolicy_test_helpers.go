//go:build test
// +build test

package strongPolicy

import (
	usage "dataManager/migrationManager/childUsage"
)

func (s *DefaultStrongCollectionPolicy) GetUnstableEquilibriumState() map[string]map[string]*usage.CircularBuffer {
	return s.equilibriumState
}

func (s *DefaultStrongCollectionPolicy) GetBuffer(collection string, child string) *usage.CircularBuffer {
	return s.equilibriumState[collection][child]
}

func (s *DefaultStrongCollectionPolicy) GetCurrentState() MigrationManagerState {
	return s.currentState
}

func (s *DefaultStrongCollectionPolicy) SetState(m MigrationManagerState) {
	s.setState(m)
}

func (s *DefaultStrongCollectionPolicy) GetDecisionState() MigrationManagerState {
	return s.decisionState
}

func (m *MigrationManagerDecisionState) ComputeReadElasticForce(cb *usage.CircularBuffer) int {
	res, _ := m.computeElasticForce(cb, usage.GetRead)
	return res
}

func (m *MigrationManagerDecisionState) ComputeWriteElasticForce(cb *usage.CircularBuffer) int {
	res, _ := m.computeElasticForce(cb, usage.GetWrite)
	return res
}
