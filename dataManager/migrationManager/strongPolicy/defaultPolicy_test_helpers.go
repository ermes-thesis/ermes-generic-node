//go:build test
// +build test

package strongPolicy

import (
	usage "dataManager/migrationManager/childUsage"
)

func (m *DefaultStrongCollectionPolicy) GetUnstableEquilibriumState() map[string]map[string]*usage.CircularBuffer {
	return m.equilibriumState
}

func (m *DefaultStrongCollectionPolicy) GetBuffer(collection string, child string) *usage.CircularBuffer {
	return m.equilibriumState[collection][child]
}

func (m *DefaultStrongCollectionPolicy) GetCurrentState() MigrationManagerState {
	return m.currentState
}

func (m *DefaultStrongCollectionPolicy) SetState(s MigrationManagerState) {
	m.setState(s)
}

func (m *DefaultStrongCollectionPolicy) GetDecisionState() MigrationManagerState {
	return m.decisionState
}

func (m *MigrationManagerDecisionState) ComputeReadElasticForce(cb *usage.CircularBuffer) int {
	res, _ := m.computeElasticForce(cb, usage.GetRead)
	return res
}

func (m *MigrationManagerDecisionState) ComputeWriteElasticForce(cb *usage.CircularBuffer) int {
	res, _ := m.computeElasticForce(cb, usage.GetWrite)
	return res
}
