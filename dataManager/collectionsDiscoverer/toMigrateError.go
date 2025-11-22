package collectionsDiscoverer

type ToMigrateError struct {
	Code    int
	Message string
}

func (e *ToMigrateError) Error() string {
	return e.Message
}
