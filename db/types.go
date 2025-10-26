package db

type MemStats struct {
	AllocationStats
	ReplicationBacklogSize uint64
	LuaScriptsCacheSize    uint64
	KeysCount              uint64
	BytesKeysRatio         uint64
}

type AllocationStats struct {
	UsedMemoryPeak              uint64
	CurrentUsedMemory           uint64
	UsedMemoryOverhead          uint64
	UsedMemoryStartup           uint64
	UsedMemoryDataset           uint64
	UsedMemoryDatasetPercentage float64
}

type RecordValue struct {
	Value     string
	Timestamp string
}
