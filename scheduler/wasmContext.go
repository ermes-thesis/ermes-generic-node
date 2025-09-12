package scheduler

import "metadataManager/utils"

type wasmContext struct {
	UserId      string
	GroupList   []string
	Collections map[string][]utils.CollectionInfo
}
