//go:build test
// +build test

package tests

import (
	usage "dataManager/migrationManager/childUsage"
	"testing"
)

func TestBuffer(t *testing.T) {
	b := usage.NewCircularBuffer(3)
	c := usage.NewChildResourceUsage()
	b.Add(c)
	b.Movehead()
	d := usage.NewChildResourceUsage()
	b.Add(d)
	b.Movehead()
	b.Add(usage.NewChildResourceUsage())
	b.Movehead()

	if b.Head != 0 {
		t.Fatal("head on wrong el - 1")
	}

	e := usage.NewChildResourceUsage()
	b.Add(e)
	b.Movehead()
	if b.Head == 0 {
		t.Fatal("head on wrong el - 2.2")
	}

	b.Add(usage.NewChildResourceUsage())
	b.Movehead()
	b.Add(usage.NewChildResourceUsage())
	b.Movehead()
	if b.Head != 0 {
		t.Fatal("head on wrong el  3")
	}
}
