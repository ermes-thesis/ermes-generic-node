//go:build test
// +build test

package lookupmanager

func ResetSingleton() {
	lookupManagerInstance = nil
}
