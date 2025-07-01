//go:build test
// +build test

package migrationManager

func (m *MigrationManagerImp) GetUnstableEquilibriumState() map[string]map[string]*childResourceUsage {
	return m.unstableEquilibriumState
}

func (s *childResourceUsage) GetWrite() resourceUsage {
	return s.write
}

func (s *childResourceUsage) GetRead() resourceUsage {
	return s.read
}
