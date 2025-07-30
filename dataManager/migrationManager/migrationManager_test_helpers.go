//go:build test
// +build test

package migrationManager

func ResetSingleton() {
	migrationManagareInstance = nil
}
