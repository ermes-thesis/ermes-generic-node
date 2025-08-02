package strongPolicy

import (
	"config"
	"dataManager/lru"
	usage "dataManager/migrationManager/childUsage"
	gossipmessages "dataManager/migrationManager/gossipMessages"
	sender "dataManager/migrationManager/migrationSender"
	penality "dataManager/migrationManager/penalityFunction"
	shared "dataManager/shared"
	"db"
	log "logger"
	"server/info"
	"sync"
	"time"
)

// default policy
type DefaultStrongCollectionPolicy struct {
	//sync
	migLock *sync.Mutex
	//config
	currentConfig config.MigrationManagerConfig
	lruConfig     config.LruConfig
	serverInfo    info.ServerInfo
	logger        log.Logger

	//state
	currentState MigrationManagerState

	idleState     MigrationManagerState
	gossipState   MigrationManagerState
	decisionState MigrationManagerState

	lruCache *lru.LRUCache[shared.Collection]

	//timing for auto transition
	stateTimer *time.Timer

	//impl
	equilibriumState map[string]map[string]*usage.CircularBuffer //mapping resource - nodeName
	partialView      *shared.CollectionPartialView               //set of local and remotes collections

	dbInterface db.GenericDB
	sender      sender.Sender

	//used to define a penalization factor for past data -> func(bufferPos int, currentValue int)
	penalityFunc penality.PenalityFunc
}

func NewDefaultPolicy(config config.MigrationManagerConfig, lruConfig config.LruConfig, collections *shared.CollectionPartialView, migLock *sync.Mutex, penalityFunc penality.PenalityFunc, dbInterface db.GenericDB, serverInfo info.ServerInfo, sender sender.Sender, lruCache *lru.LRUCache[shared.Collection], logger log.Logger) StrongCollectionPolicy {
	policy := &DefaultStrongCollectionPolicy{
		lruCache:         lruCache,
		lruConfig:        lruConfig,
		sender:           sender,
		logger:           logger,
		migLock:          migLock,
		currentConfig:    config,
		serverInfo:       serverInfo,
		partialView:      collections,
		equilibriumState: make(map[string]map[string]*usage.CircularBuffer),
		penalityFunc:     penalityFunc,
		dbInterface:      dbInterface,
	}

	policy.SetDefaultState()

	return policy
}

//impl StrongCollectionPolicy

func (s *DefaultStrongCollectionPolicy) SetDefaultState() {
	s.logger.Println("DEFAULT-STRONG-POLICY", "Setting default state", log.INFO)
	//state pattern
	s.idleState = &MigrationManagerIdleState{policy: s}
	s.gossipState = &MigrationManagerGossipState{policy: s, dbInterface: s.dbInterface, MissingMessageCounter: 0, NodeResourceUsage: make(map[string]NodeStateInfo, 0)}
	s.decisionState = &MigrationManagerDecisionState{policy: s, NodeResourceUsage: make(map[string]NodeStateInfo, 0)}

	s.currentState = s.idleState

	//start timer
	s.startIdleTimer()
}
func (s *DefaultStrongCollectionPolicy) RemoveInfo(collectionId string) {
	delete(s.equilibriumState, collectionId)
}

func (s *DefaultStrongCollectionPolicy) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	s.currentState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency)
}

func (s *DefaultStrongCollectionPolicy) ManageGossipMessage(msg gossipmessages.GossipMessage) {
	s.currentState.ManageGossipMessage(msg)
}

// methods
func (s *DefaultStrongCollectionPolicy) setState(state MigrationManagerState) {
	s.currentState = state
}

// transitionToGossip handles automatic transition from idle to gossip
func (s *DefaultStrongCollectionPolicy) transitionToGossip() {
	s.logger.Println("DEFAULT-STRONG-POLICY", "transition to gossip state", log.INFO)
	//sync
	s.migLock.Lock()
	defer s.migLock.Unlock()

	//imp
	// Only transition if still in idle state
	if _, isIdle := s.currentState.(*MigrationManagerIdleState); isIdle {
		//fmt.Println("Auto-transitioning from idle to gossip state")
		if s.serverInfo.Father != nil {
			s.logger.Println("DEFAULT-STRONG-POLICY", "fog node setted in gossip state", log.INFO)
			s.gossipState.Setup(len(s.serverInfo.Children) + 1)
		} else {
			s.logger.Println("DEFAULT-STRONG-POLICY", "cloud node setted in gossip state", log.INFO)
			s.gossipState.Setup(len(s.serverInfo.Children)) //number of adjacent nodes - TODO: manage cloud nodes.
		}
		s.setState(s.gossipState)
		s.stopIdleTimer()
	} else {
		s.logger.Println("DEFAULT-STRONG-POLICY", "can't translate to gossip state from a state different from idle", log.ERROR)
	}
}

func (s *DefaultStrongCollectionPolicy) transitionToDecision(childrenResourceUsage map[string]NodeStateInfo) {
	s.logger.Println("DEFAULT-STRONG-POLICY", "transition to decision state", log.INFO)
	// Only transition if still in gossip state
	if _, isGossip := s.currentState.(*MigrationManagerGossipState); isGossip {
		//fmt.Println("Auto-transitioning from gossip to decision state")
		s.decisionState.Setup(childrenResourceUsage)
		s.setState(s.decisionState)
	} else {
		s.logger.Println("DEFAULT-STRONG-POLICY", "can't translate to decision state from a state different from gossip", log.ERROR)
	}
}

func (s *DefaultStrongCollectionPolicy) transitionToIdle() {
	s.logger.Println("DEFAULT-STRONG-POLICY", "transition to idle state", log.INFO)

	//imp
	// Only transition if still in decision state
	if _, isDecision := s.currentState.(*MigrationManagerDecisionState); isDecision {
		//fmt.Println("Auto-transitioning from decision to idle state")
		s.setState(s.idleState)
		s.startIdleTimer()
	} else {
		s.logger.Println("DEFAULT-STRONG-POLICY", "can't translate to idel state from a state different from decision", log.ERROR)
	}
}

// startIdleTimer starts or restarts the timer for idle->gossip transition
func (s *DefaultStrongCollectionPolicy) startIdleTimer() {
	s.logger.Println("DEFAULT-STRONG-POLICY", "start idle timer", log.INFO)
	// Cancel existing timer if any
	if s.stateTimer != nil {
		s.stateTimer.Stop()
	}

	// Only start timer if currently in idle state
	if _, isIdle := s.currentState.(*MigrationManagerIdleState); isIdle {
		duration := time.Duration(s.currentConfig.IdleTimeoutMs) * time.Millisecond
		s.stateTimer = time.AfterFunc(duration, func() {
			s.transitionToGossip()
		})
	}
}

func (s *DefaultStrongCollectionPolicy) stopIdleTimer() {
	if s.stateTimer != nil {
		s.logger.Println("DEFAULT-STRONG-POLICY", "stop idle timer", log.INFO)
		s.stateTimer.Stop()
		s.stateTimer = nil
	}
}

// resetIdleTimer resets the idle timer when activity occurs
func (s *DefaultStrongCollectionPolicy) resetIdleTimer() {
	s.startIdleTimer()
}
