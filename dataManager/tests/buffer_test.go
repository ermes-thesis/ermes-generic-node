//go:build test
// +build test

package tests

import (
	migrationManager "dataManager/migrationManager"
	"testing"
)

func TestBuffer(t *testing.T) {
	b := migrationManager.NewCircularBuffer(3)
	c := migrationManager.NewChildResourceUsage()
	b.Add(c)
	b.Movehead()
	d := migrationManager.NewChildResourceUsage()
	b.Add(d)
	b.Movehead()
	b.Add(migrationManager.NewChildResourceUsage())
	b.Movehead()

	if b.Head != 0 {
		t.Fatal("head on wrong el - 1")
	}

	e := migrationManager.NewChildResourceUsage()
	b.Add(e)
	b.Movehead()
	if b.Head == 0 {
		t.Fatal("head on wrong el - 2.2")
	}

	b.Add(migrationManager.NewChildResourceUsage())
	b.Movehead()
	b.Add(migrationManager.NewChildResourceUsage())
	b.Movehead()
	if b.Head != 0 {
		t.Fatal("head on wrong el  3")
	}
}
