//go:build test
// +build test

package migrationManager

import (
	lru "dataManager/lru"
	shared "dataManager/shared"
)

func ResetSingleton() {
	migrationManagerInstance = nil
}

func (m *MigrationManagerImp) GetLru() *lru.LRUCache[shared.Collection] {
	return m.lruCache
}
