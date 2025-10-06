package scheduler

import "metadataManager/types"

type WasmContext struct {
	UserId      string
	GroupList   []string
	Collections map[string][]types.CollectionInfo
}
