package lru

import (
	"fmt"
	"testing"
)

func TestLRUCacheCreateUint(t *testing.T) {

	var size uint = 1000

	cache := LRUCacheConstructor[uint](size)

	if cache.GetTotalSize() != size {
		t.Errorf("LRU Cache total size does not match: expected: %d vs internal: %d", size, cache.GetAvailableSize())
	}

	if cache.GetCurrentSize() != 0 {
		t.Errorf("LRU Cache initial size is not zero: %d ", cache.GetCurrentSize())
	}

	if cache.GetAvailableSize() != (cache.GetTotalSize() - cache.GetCurrentSize()) {
		t.Errorf("LRU Cache error in initial GetAvailableSize method: expected: %d vs real: %d", cache.GetAvailableSize(), cache.GetTotalSize()-cache.GetCurrentSize())
	}

}

func TestLRUCachePutUint(t *testing.T) {

	var total_size uint = 1000

	var elem_size uint = 1

	cache := LRUCacheConstructor[uint](total_size)

	cache.Put("ciao", 1, elem_size)

	if cache.GetCurrentSize() != elem_size {
		t.Errorf("LRU Cache size after adding one element is not correct: expected: %d vs interal: %d", elem_size, cache.GetCurrentSize())
	}

	// Now try to add an element which does not fit

	prev_size := cache.GetCurrentSize()

	cache.Put("ciao2", 1, total_size)

	// We expect the size not to change.

	if cache.GetCurrentSize() != prev_size {
		t.Errorf("LRU Cache size after adding a too big element is not correct: expected: %d vs interal: %d", prev_size, cache.GetCurrentSize())
	}

}

func TestLRUCacheGetUint(t *testing.T) {

	var total_size uint = 1000

	var elem_size uint = 1

	cache := LRUCacheConstructor[uint](total_size)

	// Get a non existent item (the item retrieved we do not care).

	_, err1 := cache.Get("ciao")

	if err1 == nil {
		t.Errorf("LRU Cache get did not return an error when getting a non existent element.")
	}

	var put_val uint = 1

	cache.Put("ciao", put_val, elem_size)

	// Try to get the item.

	retrieved2, err2 := cache.Get("ciao")

	if err2 != nil {
		t.Errorf("LRU Cache returned an error when getting an existing value : %s", err2)
	}

	if retrieved2 != put_val {
		t.Errorf("LRU Cache returned an incorrect value: expected: %d vs returned: %d", put_val, retrieved2)
	}

}

func TestLRUCachePutLotOfElementsUint(t *testing.T) {

	var total_size uint = 5

	var elem_size uint = 1

	cache := LRUCacheConstructor[uint](total_size)

	// Fill the cache up to the last -1 element
	for i := uint(0); i < total_size/elem_size-1; i++ {
		cache.Put("ciao"+fmt.Sprint(i), i, elem_size)
	}

	// Now try to add another key, we should not be able to retrieve it. <- THIS WILL NEED TO CHANGE

	cache.Put("ciao5", 5, 2)

	_, err1 := cache.Get("ciao5")

	if err1 == nil {
		t.Errorf("Did not get an error while trying to insert an element too big for the remaining space of the cache.")
	}

	// Now add the last item.

	cache.Put("ciao4", 4, 1)

	_, err2 := cache.Get("ciao4")

	if err2 != nil {
		t.Errorf("Got an error while trying to add an element even though there is enough space: %s", err2)
	}

	// Try to add another item, it should not be possible to add it.

	cache.Put("ciao5", 5, 1)

	_, err3 := cache.Get("ciao5")

	if err3 == nil {
		t.Errorf("Did not get an error while trying to insert a new element in a full cache.")
	}
}
