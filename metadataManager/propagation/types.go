package propagation

type PropagationOrigin int

func (p PropagationOrigin) String() string {
	switch p {
	case FROM_FATHER:
		return "FROM_FATHER"
	case FROM_CHILD:
		return "FROM_CHILD"
	case FROM_LOCAL:
		return "FROM_LOCAL"
	default:
		return "UNKNOWN"
	}
}

const (
	FROM_FATHER PropagationOrigin = iota
	FROM_CHILD
	FROM_LOCAL
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
