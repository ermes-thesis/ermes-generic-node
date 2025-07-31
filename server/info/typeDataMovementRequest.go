package info

type DataMovementRequest struct {
	nodeTo string
	nodeFrom string
	key    string
	value  string // TODO: Decide real types of this objects
}

func DataMovementRequestConstructor(nodeTo string, nodeFrom string, key string, value string) DataMovementRequest {
	return DataMovementRequest{
		nodeTo: nodeTo,
		nodeFrom: nodeFrom, // node from should be constructed implicitly to be honest
		key:    key,
		value:  value,
	}
}

// At least with this wrappers the object is immutable

func (this *DataMovementRequest) getNodeTo() string{
	return this.nodeTo
}

func (this *DataMovementRequest) getNodeFrom() string{
	return this.nodeFrom
}

func (this *DataMovementRequest) getKey() string{
	return this.key
}

func (this *DataMovementRequest) getValue() string{
	return this.value
}