package info

type SimpleClientIdRequest struct {
	UserId int
}

func CreateSimpleClientIdRequest(id int) SimpleClientIdRequest {
	return SimpleClientIdRequest{
		UserId: id,
	}
}
