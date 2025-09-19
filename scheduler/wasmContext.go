package scheduler

import "metadataManager/utils"

type WasmContext struct {
	UserId      string
	GroupList   []string
	Collections map[string][]utils.CollectionInfo
}
