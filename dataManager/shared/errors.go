package dataManagerShared

type UnauthorizedError struct {
}

func (e *UnauthorizedError) Error() string {
	return "current user is not authorized"
}
