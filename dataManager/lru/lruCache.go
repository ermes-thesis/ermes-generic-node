package lru

import (
	"container/list"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// We need to create a generic LRU cache.
// We will hold an object of the type and the size of the element.
type lruCacheType[T any] struct {
	key  string
	obj  T
	size uint
}

// We just need to decide what is the key type. For the moment a string should be fine ? Another possibility is to be more generic and use comparable ?

// LRUCache This data structure is atomic.
type LRUCache[T any] struct {
	current    uint
	capacity   uint
	cache      map[string]*list.Element
	linkedlist *list.List
	mux        sync.Mutex
}

// LRUCacheConstructor Method to construct a generic LRU cache with given capacity and element type T.
func LRUCacheConstructor[T any](capacity uint) LRUCache[T] {

	return LRUCache[T]{
		current:    0,
		capacity:   capacity,
		cache:      make(map[string]*list.Element),
		linkedlist: list.New(),
		mux:        sync.Mutex{},
	}
}

// Get Return the value of the key if the key exists, otherwise return an error. This operation is atomic
func (this *LRUCache[T]) Get(key string) (T, error) {

	this.mux.Lock()

	defer this.mux.Unlock()

	get := this.cache[key]

	if get == nil {
		// Idk return error
		var zeroVal T
		return zeroVal, errors.New("key invalid")
	}

	// Push ahead in LRU

	// This I think should be O(1)
	this.linkedlist.MoveToFront(get)

	// TODO: decide if returning also the size is important

	asserted, ok := get.Value.(lruCacheType[T])

	if ok {
		return asserted.obj, nil
	} else {
		var zeroVal T
		return zeroVal, errors.New("assertion error. The value in the LRU Cache is not the correct type. It should not be possible to arrive here")
	}

}

// Put update the value of the key if the key exists. Otherwise, add the key-value pair to the cache.
// The element is marked as most recently used.
// If the number of keys exceeds the capacity from this operation, return an error (TODO: decide)
// This operation is thread safe.
func (this *LRUCache[T]) Put(key string, value T, size uint) error {

	this.mux.Lock()

	defer this.mux.Unlock()

	get := this.cache[key]

	// If the item is not present then create it, else update it
	if get == nil {

		lruCacheTypeObj := lruCacheType[T]{
			key:  key,
			obj:  value,
			size: size,
		}

		// Check the capacity. If it is full TODO: decide, we should initiate a deletion procedure
		if this.current+size > this.capacity {

			// Do nothing

			return errors.New("not enough space in the list (new item case), current: " + fmt.Sprint(this.current) + ", capacity: " + fmt.Sprint(this.capacity) + ", new size: " + fmt.Sprint(size))
		}

		// Update the size
		this.current += size

		// Add the item on top
		elem := this.linkedlist.PushFront(lruCacheTypeObj)

		// Add the list element to the map
		this.cache[key] = elem

		return nil
	}

	// Else update and put on top of LRU
	el := get.Value.(lruCacheType[T])

	if this.current-el.size+size > this.capacity {
		return errors.New("not enough space in the list (update case), current: " + fmt.Sprint(this.current) + ", capacity: " + fmt.Sprint(this.capacity) + ", old size: " + fmt.Sprint(el.size) + ", new size: " + fmt.Sprint(size))
	}

	this.current -= el.size
	el.size = size
	this.current += size

	get.Value = el

	this.linkedlist.MoveToFront(get)

	return nil
}

func (this *LRUCache[T]) MoveToFront(key string) error {

	this.mux.Lock()
	defer this.mux.Unlock()

	get := this.cache[key]
	if get == nil {
		return errors.New("key not present in the cache")
	}

	this.linkedlist.MoveToFront(get)
	return nil
}

// UpdateSize update the size of an element, without changing its position in the LRU.
// This operation is thread safe.
func (this *LRUCache[T]) UpdateSize(key string, size uint) error {

	this.mux.Lock()

	defer this.mux.Unlock()

	get := this.cache[key]
	if get == nil {
		return errors.New("key not present in the cache")
	}

	el := get.Value.(lruCacheType[T])

	//if this.current-el.size+size > this.capacity {
	//	return errors.New("not enough space in the list")
	//}

	this.current -= el.size
	el.size = size
	this.current += size

	get.Value = el

	return nil
}

// FreeToAchieveSize Utility function in order to obtain capacity-current>=sizeGoal.
// This function returns a list of removed items
func (this *LRUCache[T]) FreeToAchieveSize(sizeGoal uint) *list.List {

	this.mux.Lock()

	defer this.mux.Unlock()

	returnList := list.New()

	// Start removing elements from the back until the condition is satisfied
	for this.GetAvailableSize() <= sizeGoal {
		backElement := this.linkedlist.Back()
		if backElement == nil {
			// Lista vuota, non possiamo liberare più spazio
			break
		}
		// Pop the last element

		// Convert the last element to an object

		converted, ok := this.linkedlist.Back().Value.(lruCacheType[T])

		if !ok { // This should never happen
			return returnList
		}

		// NB: this is a "transfer of ownership" operation.
		this.linkedlist.Remove(this.linkedlist.Back())

		// Move the object to the returned list
		returnList.PushFront(converted.obj)

		// The removed element will be garbage collected (brrrr)

		// But we need to delete the key from the map
		delete(this.cache, converted.key)

		// Update the capacity
		this.current -= converted.size

	}

	return returnList
}

// Remove Function to remove a value given its key.
// This operation is thread safe and O(1)
func (this *LRUCache[T]) Remove(key string) error {

	this.mux.Lock()

	defer this.mux.Unlock()

	get := this.cache[key]

	if get == nil {
		return errors.New("trying to remove a non existing key")
	}

	// The returned element will be garbage collected (brrrr)
	this.linkedlist.Remove(get)

	// Delete the element from the map

	delete(this.cache, key)

	// Update the size

	converted, ok := get.Value.(lruCacheType[T])

	// Should never arrive here
	if !ok {
		return errors.New("Unknown error when converting element to type 'lruCacheType[T]'")
	}

	this.current -= converted.size

	return nil
}

func (this *LRUCache[T]) GetCurrentSize() uint {
	return this.current
}

func (this *LRUCache[T]) GetTotalSize() uint {
	return this.capacity
}

func (this *LRUCache[T]) GetAvailableSize() uint {
	diff := int(this.GetTotalSize()) - int(this.GetCurrentSize())
	if diff < 0 {
		return 0
	}
	return uint(diff)
}

// Potentially useful for debug, but tricky to manage with generics
// func (this *LRUCach (TODO: decide)e) PrintLRU() {

//  	fmt.Println("LRU:")
//  	fmt.Println("head: %d", this.head.key)
// 	fmt.Println("tail: %d", this.tail.key)

//  	fmt.Println("Forwards:")
//  	for i := this.head; i != nil; i = i.next {
//  		fmt.Println("%d", i.key)
//  	}

//  	fmt.Println("Backwards:")

//  	for i := this.tail; i != nil; i = i.prev {
//  		fmt.Println("%d", i.key)
// 	}
// }

func PrettyPrintList(lst *list.List) string {
	var sb strings.Builder
	sb.WriteString("[")
	for e := lst.Front(); e != nil; e = e.Next() {
		sb.WriteString(fmt.Sprintf("%+v", e.Value))
		if e.Next() != nil {
			sb.WriteString(",")
		}
	}

	sb.WriteString("]")
	return sb.String()
}
