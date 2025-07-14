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
	collectionId    string
	collectionState CollectionState
	IsLeader        bool
	LeaderId        string
}

func NewStrongCollection(collectionId string, size int, isRemote bool) Collection {
	hosts := make(map[string]struct{})
	if isRemote {
		return &StrongCollection{
			collectionState: NewRemoteCollection(hosts),
		}
	}
	return &StrongCollection{
		collectionState: NewLocalCollection(size, hosts),
	}
}

func (s *StrongCollection) SetToLocal(size int) {
	if s.collectionState.IsLocal() {
		return
	}
	collectionState := NewLocalCollection(size, s.collectionState.HostsId())
	s.collectionState = collectionState
}

func (s *StrongCollection) SetToRemote(hostNames map[string]struct{}) {
	if !s.collectionState.IsLocal() {
		return
	}

	// Merge additionalHosts
	for host := range hostNames {
		s.collectionState.AddHost(host)
	}

	collectionState := NewRemoteCollection(s.collectionState.HostsId())
	s.collectionState = collectionState
}

func (s *StrongCollection) CollectionId() string {
	return s.collectionId
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
	collectionId    string
	collectionState CollectionState
}

func NewWeakCollection(collectionId string, size int, isRemote bool) Collection {
	hosts := make(map[string]struct{})
	if isRemote {
		return &WeakCollection{
			collectionState: NewRemoteCollection(hosts),
		}
	}
	return &WeakCollection{
		collectionState: NewLocalCollection(size, hosts),
	}
}

func (s *WeakCollection) SetToLocal(size int) {
	if s.collectionState.IsLocal() {
		return
	}
	collectionState := NewLocalCollection(size, s.collectionState.HostsId())
	s.collectionState = collectionState
}

func (w *WeakCollection) SetToRemote(hostNames map[string]struct{}) {
	if !w.collectionState.IsLocal() {
		return
	}

	// Merge additionalHosts
	for host := range hostNames {
		w.collectionState.AddHost(host)
	}

	collectionState := NewRemoteCollection(w.collectionState.HostsId())
	w.collectionState = collectionState
}

func (w *WeakCollection) CollectionId() string {
	return w.collectionId
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
