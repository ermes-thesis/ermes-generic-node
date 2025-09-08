package resourcemanager

import (
	"runtime"
)

// Interesting but brief blog: https://reintech.io/blog/introduction-to-gos-runtime-package-memory-management-performance

// GetRuntimeMemoryAllocated Function which returns the current memory allocated by the node.
func GetRuntimeMemoryAllocated() uint64 {

	var stats runtime.MemStats

	runtime.ReadMemStats(&stats)

	// Careful this is the total heap currently allocated and non free-ed.
	return stats.HeapAlloc

}
