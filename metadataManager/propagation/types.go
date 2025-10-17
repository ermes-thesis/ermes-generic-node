package propagation

type PropagationOrigin int

func (p PropagationOrigin) String() string {
	switch p {
	case FROM_FATHER:
		return "FATHER"
	case FROM_CHILD:
		return "CHILD"
	case FROM_LOCAL:
		return "LOCAL"
	default:
		return "UNKNOWN"
	}
}

const (
	FROM_FATHER PropagationOrigin = iota
	FROM_CHILD
	FROM_LOCAL
)

type MetadataAction int

const (
	Add = iota
	Remove
)

type UpdateGroupResponse struct {
	Groups   []string `json:"groups"`
	Sequence uint64   `json:"sequence"`
}

type GroupMessage struct {
	UserGroups     []string `json:"userGroups"`
	GroupsToAdd    []string `json:"groupsToAdd,omitempty"`
	GroupsToRemove []string `json:"groupsToRemove,omitempty"`
}

const (
	EndpointLookup              = "/pvt-lookup"
	EndpointCreateMetadata      = "/pvt-create-metadata"
	EndpointRequestUpdateGroups = "/pvt-request-update-groups"
	EndpointPropagateGroups     = "/pvt-propagate-groups"
)
