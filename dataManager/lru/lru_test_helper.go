//go:build test
// +build test

package lru

import "container/list"

func (this *LRUCache[T]) GetLinkedList() *list.List {
	return this.linkedlist
}

func (this *LRUCache[T]) CheckOrder(keys []string) bool {
	this.mux.Lock()
	defer this.mux.Unlock()

	if len(keys) == 0 {
		return true
	}

	current := this.linkedlist.Front()
	matchIndex := 0

	for current != nil && matchIndex < len(keys) {
		node := current.Value.(lruCacheType[T])
		if node.key == keys[matchIndex] {
			matchIndex++
		}
		current = current.Next()
	}

	return matchIndex == len(keys)
}
