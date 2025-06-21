package clientrepo

import (
	"sync"
)

type ClientRepo struct {
	mux     sync.Mutex
	clients map[int]int
	num     int
}

func CreateClientRepo() *ClientRepo {

	return &ClientRepo{
		sync.Mutex{},
		make(map[int]int),
		0,
	}

}

func CreateUser(repo *ClientRepo) (int, error) {

	// TODO: this operation can only be done in the cloud nodes.

	repo.mux.Lock()

	// TODO: Here we should contact the other cloud regions to check if the user is already present somewhere else.

	curr := repo.num

	repo.clients[repo.num] = 1

	repo.num++

	repo.mux.Unlock()

	return curr, nil

}

func AddUser(repo *ClientRepo, userId int) error {

	repo.mux.Lock()

	// If the user was already present do nothing for the moment.
	repo.clients[userId] = 1

	repo.mux.Unlock()

	return nil
}

func DeleteUser(repo *ClientRepo, userId int) error {

	repo.mux.Lock()

	// TODO: notify the other cloud regions too.

	delete(repo.clients, userId)

	repo.mux.Unlock()

	return nil

}

func IsUserPresent(repo *ClientRepo, userId int) bool {
	// TODO: make this an atomic function (maybe its not needed)
	return repo.clients[userId] == 1
}
