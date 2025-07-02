package dataManagerShared

// collection Info
type Collection interface {
	CollectionId() string //duplicated?
	IsLocal() bool
}

type LocalCollection struct {
	collectionId string
	size         int
}

func (l *LocalCollection) CollectionId() string {
	return l.collectionId
}
func (l *LocalCollection) IsLocal() bool {
	return true
}
func (l *LocalCollection) Size() int {
	return l.size
}

type RemoteCollection struct {
	collectionId string
	hostsId      []string //TODO maybe insert latency with host or maybe not idk
}

func (l *RemoteCollection) CollectionId() string {
	return l.collectionId
}
func (l *RemoteCollection) IsLocal() bool {
	return false
}
func (l *RemoteCollection) HostsId() []string {
	return l.hostsId
}

// partialView
type CollectionPartialView struct {
	KnowedCollections map[string]*Collection //maps collectionID to info related to that collection
}
