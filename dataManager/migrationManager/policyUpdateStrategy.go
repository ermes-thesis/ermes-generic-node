package migrationManager

import (
	"dataManager/messages"
	shared "dataManager/shared"
)

type policyUpdateStrategy interface {
	updatesRequestsInfo(info []messages.CollectionRequestInfo, sender string, m *MigrationManagerImp)
	updateCollectionsLatestUsage(collections []string, m *MigrationManagerImp)
}

type noUpdateStrategy struct{}

func NewNoUpdateStrategy() policyUpdateStrategy {
	return &noUpdateStrategy{}
}

func (n noUpdateStrategy) updatesRequestsInfo(info []messages.CollectionRequestInfo, sender string, m *MigrationManagerImp) {
}
func (n noUpdateStrategy) updateCollectionsLatestUsage(collections []string, m *MigrationManagerImp) {
}

type ermesPolicyUpdateStrategy struct{}

func NewErmesPolicyUpdateStrategy() policyUpdateStrategy {
	return &ermesPolicyUpdateStrategy{}
}

func (e ermesPolicyUpdateStrategy) updatesRequestsInfo(info []messages.CollectionRequestInfo, sender string, m *MigrationManagerImp) {
	m.updatesRequestsInfo(messages.ToCollectionRequestInfoMessages(info, sender))
}
func (e ermesPolicyUpdateStrategy) updateCollectionsLatestUsage(collections []string, m *MigrationManagerImp) {
	//sync
	m.collectionsLock.Lock()
	defer m.collectionsLock.Unlock()

	for _, c := range collections {
		collection := m.collectionsPartialView.KnownCollections[c]
		if collection != nil && collection.IsLocal() {
			m.updateLru(c, uint(collection.GetCollectionState().(*shared.LocalCollection).Size()))
		}
	}
}
