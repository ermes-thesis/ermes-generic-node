//go:build test
// +build test

package collectionsDiscoverer

import "sync"

func ResetSingleton() {
	collectionsDiscovererInstance = nil
	once = sync.Once{}
	initOnce = sync.Once{}
}
