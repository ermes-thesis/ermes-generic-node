//go:build test
// +build test

package collectionsDiscoverer

func ResetSingleton() {
	lookupManagerInstance = nil
}
