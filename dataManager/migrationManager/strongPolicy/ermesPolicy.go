package strongPolicy

import (
	"config"
	"dataManager/lru"
	gossipmessages "dataManager/messages"
	penality "dataManager/migrationManager/penalityFunction"
	sender "dataManager/sender"
	shared "dataManager/shared"
	"db"
	"server"
	"sync"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
)

// ErmesStrongCollectionPolicy default policy
type ErmesStrongCollectionPolicy struct {
	//sync
	migLock *sync.Mutex
	//config
	currentConfig config.MigrationManagerConfig
	lruConfig     config.LruConfig
	serverInfo    server.ServerInfo
	log           logger.Logger

	//state
	currentState MigrationManagerState

	idleState     MigrationManagerState
	gossipState   MigrationManagerState
	decisionState MigrationManagerState

	lruCache *lru.LRUCache[shared.Collection]

	//timing for auto transition
	stateTimer *time.Timer

	//impl
	//equilibriumState map[string]map[string]*usage.CircularBuffer OLD
	equilibriumState MyMap                         // key: string (resource), value: map[string]*usage.CircularBuffer mapping resource - nodeName
	partialView      *shared.CollectionPartialView //set of local and remotes collections

	dbInterface db.GenericDB
	sender      sender.MigrationManagerSender

	//used to define a penalization factor for past data -> func(bufferPos int, currentValue int)
	penalityFunc penality.PenalityFunc
}

func NewDefaultPolicy(config config.MigrationManagerConfig, lruConfig config.LruConfig, collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, dbInterface db.GenericDB, serverInfo server.ServerInfo, sender sender.MigrationManagerSender, lruCache *lru.LRUCache[shared.Collection], logger logger.Logger) StrongCollectionPolicy {
	policy := &ErmesStrongCollectionPolicy{
		lruCache:         lruCache,
		lruConfig:        lruConfig,
		sender:           sender,
		log:              logger,
		migLock:          &sync.Mutex{},
		currentConfig:    config,
		serverInfo:       serverInfo,
		partialView:      collections,
		equilibriumState: MyMap{},
		penalityFunc:     penalityFunc,
		dbInterface:      dbInterface,
	}

	policy.SetDefaultState()

	return policy
}

//impl StrongCollectionPolicy

func (s *ErmesStrongCollectionPolicy) SetDefaultState() {
	s.log.Info("[DEFAULT-STRONG-POLICY], Setting default state")
	//state pattern
	s.idleState = &MigrationManagerIdleState{policy: s}
	s.gossipState = &MigrationManagerGossipState{policy: s, dbInterface: s.dbInterface, MissingMessageCounter: 0}
	s.decisionState = &MigrationManagerDecisionState{policy: s}

	s.currentState = s.idleState

	//start timer
	s.startIdleTimer()
}
func (s *ErmesStrongCollectionPolicy) RemoveInfo(collectionId string) {
	s.equilibriumState.Delete(collectionId)
}

func (s *ErmesStrongCollectionPolicy) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	s.currentState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency)
}

func (s *ErmesStrongCollectionPolicy) ManageGossipMessage(msg gossipmessages.GossipMessage) {
	s.currentState.ManageGossipMessage(msg)
}

// methods
func (s *ErmesStrongCollectionPolicy) setState(state MigrationManagerState) {
	s.currentState = state
}

// transitionToGossip handles automatic transition from idle to gossip
func (s *ErmesStrongCollectionPolicy) transitionToGossip() {
	s.log.Eval("[DEFAULT-STRONG-POLICY], transition to gossip state")
	//sync
	s.migLock.Lock()
	defer s.migLock.Unlock()

	//imp
	// Only transition if still in idle state
	if _, isIdle := s.currentState.(*MigrationManagerIdleState); isIdle {
		//fmt.Println("Auto-transitioning from idle to gossip state")
		s.setState(s.gossipState)
		s.stopIdleTimer()
		if s.serverInfo.Father != nil {
			s.log.Info("DEFAULT-STRONG-POLICY, fog node set in gossip state")
			s.gossipState.Setup(len(s.serverInfo.Children) + 1)
		} else {
			s.log.Info("DEFAULT-STRONG-POLICY, cloud node set in gossip state")
			s.gossipState.Setup(len(s.serverInfo.Children)) //number of adjacent nodes - TODO: manage cloud nodes.
		}
	} else {
		s.log.Error("DEFAULT-STRONG-POLICY, can't translate to gossip state from a state different from idle")
	}
}

func (s *ErmesStrongCollectionPolicy) transitionToDecision() {
	s.log.Eval("DEFAULT-STRONG-POLICY, transition to decision state")
	// Only transition if still in gossip state
	if _, isGossip := s.currentState.(*MigrationManagerGossipState); isGossip {
		//fmt.Println("Auto-transitioning from gossip to decision state")
		s.setState(s.decisionState)
		s.decisionState.Setup()

	} else {
		s.log.Error("DEFAULT-STRONG-POLICY, can't translate to decision state from a state different from gossip")
	}
}

func (s *ErmesStrongCollectionPolicy) transitionToIdle() {
	s.log.Eval("DEFAULT-STRONG-POLICY, transition to idle state")

	//imp
	// Only transition if still in decision state
	if _, isDecision := s.currentState.(*MigrationManagerDecisionState); isDecision {
		//fmt.Println("Auto-transitioning from decision to idle state")
		s.setState(s.idleState)
		s.startIdleTimer()
	} else {
		s.log.Error("DEFAULT-STRONG-POLICY, can't translate to idle state from a state different from decision")
	}
}

// startIdleTimer starts or restarts the timer for idle->gossip transition
func (s *ErmesStrongCollectionPolicy) startIdleTimer() {
	s.log.Info("DEFAULT-STRONG-POLICY, start idle timer")
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

func (s *ErmesStrongCollectionPolicy) stopIdleTimer() {
	if s.stateTimer != nil {
		s.log.Info("DEFAULT-STRONG-POLICY, stop idle timer")
		s.stateTimer.Stop()
		s.stateTimer = nil
	}
}

// resetIdleTimer resets the idle timer when activity occurs
func (s *ErmesStrongCollectionPolicy) resetIdleTimer() {
	s.startIdleTimer()
}
