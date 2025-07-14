package dataManagerShared

// collection Info
type Collection interface {
	CollectionId() string //duplicated?
	IsLocal() bool
	IsStrong() bool
	AddHost(hostId string)
	HostsId() map[string]struct{}
	SetToLocal(int)
	SetToRemote(map[string]struct{})
}

// consistency levels
// strong collection
type StrongCollection struct {
	collectionState CollectionState
	IsLeader        bool
	LeaderId        string
}

func NewStrongCollection(collectionId string, size int, isRemote bool) Collection {
	hosts := make(map[string]struct{})
	if isRemote {
		return &StrongCollection{
			collectionState: NewRemoteCollection(collectionId, hosts),
		}
	}
	return &StrongCollection{
		collectionState: NewLocalCollection(collectionId, size, hosts),
	}
}

func (s *StrongCollection) SetToLocal(size int) {
	if s.collectionState.IsLocal() {
		return
	}
	collectionState := NewLocalCollection(s.collectionState.CollectionId(), size, s.collectionState.HostsId())
	s.collectionState = collectionState
}

func (s *StrongCollection) SetToRemote(hostNames map[string]struct{}) {
	panic("unimplemented") //TODO
}

func (s *StrongCollection) CollectionId() string {
	return s.collectionState.CollectionId()
}
func (s *StrongCollection) AddHost(hostId string) {
	s.collectionState.AddHost(hostId)
}
func (s *StrongCollection) IsLocal() bool {
	return s.collectionState.IsLocal()
}
func (s *StrongCollection) IsStrong() bool {
	return true
}
func (s *StrongCollection) HostsId() map[string]struct{} {
	return s.collectionState.HostsId()
}

// weak collection
type WeakCollection struct {
	collectionState CollectionState
}

func NewWeakCollection(collectionId string, size int, isRemote bool) Collection {
	hosts := make(map[string]struct{})
	if isRemote {
		return &WeakCollection{
			collectionState: NewRemoteCollection(collectionId, hosts),
		}
	}
	return &WeakCollection{
		collectionState: NewLocalCollection(collectionId, size, hosts),
	}
}

func (s *WeakCollection) SetToLocal(size int) {
	if s.collectionState.IsLocal() {
		return
	}
	collectionState := NewLocalCollection(s.collectionState.CollectionId(), size, s.collectionState.HostsId())
	s.collectionState = collectionState
}

func (w *WeakCollection) SetToRemote(hostNames map[string]struct{}) {
	panic("unimplemented") //TODO
}

func (w *WeakCollection) CollectionId() string {
	return w.collectionState.CollectionId()
}
func (s *WeakCollection) AddHost(hostId string) {
	s.collectionState.AddHost(hostId)
}
func (w *WeakCollection) IsLocal() bool {
	return w.collectionState.IsLocal()
}
func (w *WeakCollection) IsStrong() bool {
	return false
}
func (w *WeakCollection) HostsId() map[string]struct{} {
	return w.collectionState.HostsId()
}
