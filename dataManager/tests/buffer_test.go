//go:build test
// +build test

package tests

import (
	usage "dataManager/migrationManager/childUsage"
	"testing"
)

func TestBuffer(t *testing.T) {
	b := usage.NewCircularBuffer(3)
	b.MoveHead()
	b.MoveHead()
	b.MoveHead()

	if b.HeadCursor() != 0 {
		t.Fatal("head on wrong el - 1")
	}

	b.MoveHead()
	if b.HeadCursor() == 0 {
		t.Fatal("head on wrong el - 2.2")
	}

	b.MoveHead()
	b.MoveHead()
	if b.HeadCursor() != 0 {
		t.Fatal("head on wrong el  3")
	}
}
