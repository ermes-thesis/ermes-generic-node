package strongPolicy

import (
	"config"
	usage "dataManager/migrationManager/childUsage"
	gossipmessages "dataManager/migrationManager/gossipMessages"
	sender "dataManager/migrationManager/migrationSender"
	penality "dataManager/migrationManager/penalityFunction"
	shared "dataManager/shared"
	"db"
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
	serverInfo    info.ServerInfo

	//state
	currentState MigrationManagerState

	idleState     MigrationManagerState
	gossipState   MigrationManagerState
	decisionState MigrationManagerState

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

func NewDefaultPolicy(config config.MigrationManagerConfig, collections *shared.CollectionPartialView, migLock *sync.Mutex, penalityFunc penality.PenalityFunc, dbInterface db.GenericDB) StrongCollectionPolicy {
	return &DefaultStrongCollectionPolicy{
		migLock:          migLock,
		currentConfig:    config,
		partialView:      collections,
		equilibriumState: make(map[string]map[string]*usage.CircularBuffer),
		penalityFunc:     penalityFunc,
		dbInterface:      dbInterface,
	}
}

//impl StrongCollectionPolicy

func (s *DefaultStrongCollectionPolicy) SetDefaultState() {
	//state pattern
	s.idleState = &MigrationManagerIdleState{policy: s}
	//s.gossipState = &MigrationManagerGossipState{policy: s, MissingMessageCounter: 0, ChildrenResourceUsage: make(map[string]ChildStateInfo, 0)}
	s.decisionState = &MigrationManagerDecisionState{policy: s, ChildStateInfo: make(map[string]ChildStateInfo, 0)}

	s.currentState = s.idleState

	//start timer
	s.startIdleTimer()
}

func (s *DefaultStrongCollectionPolicy) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	panic("unimplemented")
}

func (s *DefaultStrongCollectionPolicy) ManageGossipMessage(msg gossipmessages.GossipMessage) {
	panic("unimplemented")
}

// methods
func (s *DefaultStrongCollectionPolicy) setState(state MigrationManagerState) {
	s.currentState = state
}

// transitionToGossip handles automatic transition from idle to gossip
func (s *DefaultStrongCollectionPolicy) transitionToGossip() {
	//sync
	s.migLock.Lock()
	defer s.migLock.Unlock()

	//imp
	// Only transition if still in idle state
	if _, isIdle := s.currentState.(*MigrationManagerIdleState); isIdle {
		//fmt.Println("Auto-transitioning from idle to gossip state")
		if s.serverInfo.Father != nil {
			s.gossipState.Setup(len(s.serverInfo.Children) + 1)
		} else {
			s.gossipState.Setup(len(s.serverInfo.Children)) //number of adjacent nodes - TODO: manage cloud nodes.
		}
		s.setState(s.gossipState)
		s.stopIdleTimer()
	}
}

func (s *DefaultStrongCollectionPolicy) transitionToDecision(childrenResourceUsage map[string]ChildStateInfo) {
	// Only transition if still in gossip state
	if _, isGossip := s.currentState.(*MigrationManagerGossipState); isGossip {
		//fmt.Println("Auto-transitioning from gossip to decision state")
		s.decisionState.Setup(childrenResourceUsage)
		s.setState(s.decisionState)
	}
}

func (s *DefaultStrongCollectionPolicy) transitionToIdle() {
	//sync
	s.migLock.Lock()
	defer s.migLock.Unlock()

	//imp
	// Only transition if still in decision state
	if _, isDecision := s.currentState.(*MigrationManagerDecisionState); isDecision {
		//fmt.Println("Auto-transitioning from decision to idle state")
		s.setState(s.idleState)
		s.startIdleTimer()
	}
}

// startIdleTimer starts or restarts the timer for idle->gossip transition
func (s *DefaultStrongCollectionPolicy) startIdleTimer() {
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
		s.stateTimer.Stop()
		s.stateTimer = nil
	}
}

// resetIdleTimer resets the idle timer when activity occurs
func (s *DefaultStrongCollectionPolicy) resetIdleTimer() {
	s.startIdleTimer()
}
