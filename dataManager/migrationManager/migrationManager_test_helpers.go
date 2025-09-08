//go:build test
// +build test

package migrationManager

import (
	lru "dataManager/lru"
	shared "dataManager/shared"
	"sync"
)

func ResetSingleton() {
	migrationManagerInstance = nil
	once = sync.Once{}
	initOnce = sync.Once{}
}

func (m *MigrationManagerImp) GetLru() *lru.LRUCache[shared.Collection] {
	return m.lruCache
}
