package migrationManager

import (
	"dataManager/messages"
)

type policyUpdateStrategy interface {
	updatesRequestsInfo(info []messages.CollectionRequestInfo, sender string, m *MigrationManagerImp)
	updateCollectionsLatestUsage(collections []string, m *MigrationManagerImp)
}

type noUpdateStrategy struct{}

func newNoUpdateStrategy() policyUpdateStrategy {
	return &noUpdateStrategy{}
}

func (n noUpdateStrategy) updatesRequestsInfo(info []messages.CollectionRequestInfo, sender string, m *MigrationManagerImp) {
}
func (n noUpdateStrategy) updateCollectionsLatestUsage(collections []string, m *MigrationManagerImp) {
}

type ermesPolicyUpdateStrategy struct{}

func newErmesPolicyUpdateStrategy() policyUpdateStrategy {
	return &ermesPolicyUpdateStrategy{}
}

func (e ermesPolicyUpdateStrategy) updatesRequestsInfo(info []messages.CollectionRequestInfo, sender string, m *MigrationManagerImp) {
	m.updatesRequestsInfo(messages.ToCollectionRequestInfoMessages(info, sender))
}
func (e ermesPolicyUpdateStrategy) updateCollectionsLatestUsage(collections []string, m *MigrationManagerImp) {
	//sync
	//m.mutex.Lock()
	//defer m.mutex.Unlock()

	for _, c := range collections {
		collection, exists := m.collectionsPartialView.GetCollection(c)
		if exists && collection.IsLocal() {
			err := m.lruCache.MoveToFront(c)
			if err != nil {
				m.log.Warn("Error moving to front in LRU cache", "collection_id", c, "error", err)
			}
		}
	}
}
