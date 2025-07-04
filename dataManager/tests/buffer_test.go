package tests

import (
	migrationManager "dataManager/migrationManager"
	shared "dataManager/shared"
	"testing"
)

func TestBuffer(t *testing.T) {
	b := migrationManager.NewCircularBuffer(3)
	c := shared.NewLocalCollection("repoId", 10, make(map[string]struct{}))
	b.Add(c)
	d := shared.NewLocalCollection("repoId", 10, make(map[string]struct{}))
	b.Add(d)
	b.Add(shared.NewLocalCollection("repoId", 10, make(map[string]struct{})))

	if b.Data[b.Head] != c || b.Head != 0 {
		t.Fatal("head on wrong el 1")
	}

	e := shared.NewLocalCollection("repoId", 10, make(map[string]struct{}))
	b.Add(e)
	if b.Data[b.Head] != d || b.Head == 0 || b.Data[b.Head] == c {
		t.Fatal("head on wrong el 2")
	}
	b.Add(shared.NewLocalCollection("repoId", 10, make(map[string]struct{})))
	b.Add(shared.NewLocalCollection("repoId", 10, make(map[string]struct{})))
	if b.Data[b.Head] != e || b.Head != 0 || b.Data[b.Head] == c {
		t.Fatal("head on wrong el  3")
	}
}
