package lru

import (
	"testing"
)

func TestLRUCacheCreate(t *testing.T){

	var size uint = 1000

	cache:= LRUCacheConstructor[uint](size)

	if (cache.GetTotalSize() != size){
		t.Errorf("LRU Cache total size does not match: expected: %d vs internal: %d",size, cache.GetAvailableSize())
	}

	if(cache.GetCurrentSize() != 0){
		t.Errorf("LRU Cache initial size is not zero: %d ", cache.GetCurrentSize())
	}

	if(cache.GetAvailableSize() != (cache.GetTotalSize()-cache.GetCurrentSize())){
		t.Errorf("LRU Cache error in initial GetAvailableSize method: expected: %d vs real: %d",cache.GetAvailableSize(), cache.GetTotalSize()-cache.GetCurrentSize())
	}

}

func TestLRUCachePut(t *testing.T){
	
	var total_size uint = 1000

	var elem_size uint = 1

	cache:= LRUCacheConstructor[uint](total_size)

	cache.Put("ciao", 1, elem_size)

	if(cache.GetCurrentSize()!=elem_size){
		t.Errorf("LRU Cache size after adding one element is not correct: expected: %d vs interal: %d", elem_size, cache.GetCurrentSize())
	}

	// Now try to add an element which does not fit

	prev_size := cache.GetCurrentSize()

	cache.Put("ciao2",1, total_size)

	// We expect the size not to change

	if(cache.GetCurrentSize()!=prev_size){
		t.Errorf("LRU Cache size after adding a too big element is not correct: expected: %d vs interal: %d", prev_size, cache.GetCurrentSize())
	}

}

func TestLRUCacheGet(t *testing.T){
	
	var total_size uint = 1000

	var elem_size uint = 1

	cache:= LRUCacheConstructor[uint](total_size)

	// Get a non existent item (the item retrieved we do not care)

	_ , err1 := cache.Get("ciao")

	if(err1==nil){
		t.Errorf("LRU Cache get did not return an error when getting a non existent element")
	}

	var put_val uint = 1

	cache.Put("ciao", put_val, elem_size)

	// Try to get the item

	retrieved2 , err2 := cache.Get("ciao")

	if(err2 != nil){
		t.Errorf("LRU Cache returned an error when getting an existing value : %s",err2)
	}

	if(retrieved2 != put_val){
		t.Errorf("LRU Cache returned an incorrect value: expected: %d vs returned: %d", put_val, retrieved2)
	}

}