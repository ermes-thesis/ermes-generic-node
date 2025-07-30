package migrationManager

import (
	"config"
	lru "dataManager/lru"
	gossip "dataManager/migrationManager/gossipMessages"
	penality "dataManager/migrationManager/penalityFunction"
	policy "dataManager/migrationManager/strongPolicy"
	shared "dataManager/shared"
	db "db"
	info "server/info"
	"sync"
)

type MigrationManager interface {
	//inizialize thje singleto pattern
	Init(*shared.CollectionPartialView, penality.PenalityFunc, config.MigrationManagerConfig, info.ServerInfo, *lru.LRUCache[shared.Collection], db.GenericDB)
	// used to update lru in real time (collectionID)
	// if the collection is strong used to update forces in real time (collectionID, nodeName, write?, request cumulative latency).
	// if the request comes from a user => nodeName = "user"
	ManageNewRequest(string, string, bool, int)
	//used to update non real time info
	ManageGossipMessage(gossip.GossipMessage)
	//used when the node receive  a collection from one of the directly connected nodes (collectionID,size,isStrong)
	//return True if the collection  is already stored locally and must not be stored in Redis
	NewCollectionReceived(string, int, bool, map[string]string, []string) bool

	ChangeStrongCollectionPolicy(policy.CurrentPolicy)
}

type MigrationManagerImp struct {
	//config
	config     config.MigrationManagerConfig
	serverInfo info.ServerInfo

	//policy
	defaultPolicy          policy.StrongCollectionPolicy
	StrongCollectionPolicy policy.StrongCollectionPolicy

	//impl
	collectionsPartialView *shared.CollectionPartialView //set of local and remotes collections

	lruCache    *lru.LRUCache[shared.Collection]
	dbInterface db.GenericDB
}

// singleton pattern
var migrationManagareInstance MigrationManager
var migLock = &sync.Mutex{}

//TODO: remove from global var

func GetMigrationManagerInstance() MigrationManager {
	if migrationManagareInstance == nil {
		migLock.Lock()
		defer migLock.Unlock()
		if migrationManagareInstance == nil {
			migrationManagareInstance = &MigrationManagerImp{}
		}
	}
	return migrationManagareInstance
}

func (m *MigrationManagerImp) Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB) {
	//sync
	migLock.Lock()
	defer migLock.Unlock()
	//imp
	if m.collectionsPartialView != nil { //if singleton allready inizialized
		return
	}
	m.serverInfo = serverInfo
	m.dbInterface = dbInterface
	m.lruCache = lruCache
	m.config = config

	//policy
	m.defaultPolicy = policy.NewDefaultPolicy(config, collections, migLock, penalityFunc, dbInterface, serverInfo)
	m.StrongCollectionPolicy = m.defaultPolicy

	//init data
	m.collectionsPartialView = collections
}

// methods
func (m *MigrationManagerImp) NewCollectionReceived(collectionId string, collectionSize int, isStrong bool, records map[string]string, groups []string) bool {
	collection := m.collectionsPartialView.KnowedCollections[collectionId]
	if collection != nil {
		if collection.IsLocal() {
			return true
			//TODO probably for weak collections conflict must be solved in this case
		}
		err := m.dbInterface.SetCollection(collectionId, records, groups) //add collection to redis
		if err != nil {
			panic(err) //TODO: decide
		}

		collection.SetToLocal(collectionSize)
		return false
	}
	m.dbInterface.SetCollection(collectionId, records, groups) //add collection to redis
	if isStrong {
		m.collectionsPartialView.KnowedCollections[collectionId] = shared.NewStrongCollection(collectionId, collectionSize, false) //TODO leader?
	} else {
		m.collectionsPartialView.KnowedCollections[collectionId] = shared.NewWeakCollection(collectionId, collectionSize, false)
	}
	return false
}

func (m *MigrationManagerImp) ManageGossipMessage(msg gossip.GossipMessage) {
	m.StrongCollectionPolicy.ManageGossipMessage(msg)
}

func (m *MigrationManagerImp) ManageNewRequest(collectionId string, nodeName string, isWrite bool, requestLatency int) { //error{
	//if no local info on the collection
	if m.collectionsPartialView.KnowedCollections[collectionId] == nil || !m.collectionsPartialView.KnowedCollections[collectionId].IsLocal() {
		return //lancia errore
	}

	//place on top in the LRU
	m.lruCache.Put(collectionId, m.collectionsPartialView.KnowedCollections[collectionId], uint(m.collectionsPartialView.KnowedCollections[collectionId].GetCollectionState().(*shared.LocalCollection).Size()))

	//if the collection is strong update forces
	if m.collectionsPartialView.KnowedCollections[collectionId].IsStrong() {
		m.StrongCollectionPolicy.ManageNewStrongRequest(collectionId, nodeName, isWrite, requestLatency)
	}
}

func (m *MigrationManagerImp) ChangeStrongCollectionPolicy(newPolicy policy.CurrentPolicy) {
	switch newPolicy {
	case policy.DefaultPolicy:
		m.StrongCollectionPolicy = m.defaultPolicy
	default:
		panic("unknown policy")
	}
}
