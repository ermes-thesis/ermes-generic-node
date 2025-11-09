package scheduler

import "github.com/ermes-thesis/ermes-common/model"

type WasmContext struct {
	UserId      string
	GroupList   []string
	Collections map[string][]model.CollectionInfo
}
