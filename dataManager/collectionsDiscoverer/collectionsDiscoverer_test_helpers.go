//go:build test
// +build test

package collectionsDiscoverer

func ResetSingleton() {
	collectionsDiscovererInstance = nil
}
