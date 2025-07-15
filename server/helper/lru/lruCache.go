package lru

import (
	"errors"
)

// We need to create a generic LRU cache.
// We will hold an object of the type and the size of the element.

type lruCacheType[T any] struct {
	obj  T
	size uint
}

// We just need to decide what is the key type. For the moment a string should be fine ? Another possibility is to be more generic and use comparable ?

// TODO: there already exists a double linked list type.

// TODO: make this atomic

type LinkedList[T any] struct {
	key  string
	val  lruCacheType[T]
	next *LinkedList[T]
	prev *LinkedList[T]
}

type LRUCache[T any] struct {
	current  uint
	capacity uint
	cache    map[string]*LinkedList[T]
	head     *LinkedList[T]
	tail     *LinkedList[T]
}

func LRUCacheConstructor[T any](capacity uint) LRUCache[T] {

	return LRUCache[T]{
		current:  0,
		capacity: capacity,
		cache:    make(map[string]*LinkedList[T]),
		head:     nil,
		tail:     nil,
	}
}

// Return the value of the key if the key exists, otherwise return an error.
func (this *LRUCache[T]) Get(key string) (T, error) {

	get := this.cache[key]

	if get == nil {
		// Idk return error
		var zeroVal T
		return zeroVal, errors.New("key invalid")
	}

	// Push ahead in LRU

	// If the element is first then do nothing

	if get != this.head {

		// Save next and prev
		next := get.next
		prev := get.prev

		// Remove get from list
		prev.next = next

		// If it is in the middle then fix also reverse
		if next != nil {
			next.prev = prev
		}

		// If the element was the tail fix the tail

		if get == this.tail {
			this.tail = prev
		}

		// get now becomes the head
		get.next = this.head
		get.prev = nil

		// Fix the previous of the head
		this.head.prev = get

		// Attach the get to the head
		this.head = get

	}

	// TODO: decide if returning also the size is important
	return get.val.obj, nil

}

// Update the value of the key if the key exists. Otherwise, add the key-value pair to the cache.
// If the number of keys exceeds the capacity from this operation, return an error (TODO: decide)
func (this *LRUCache[T]) Put(key string, value T, size uint) {

	get := this.cache[key]

	lruCacheTypeObj := lruCacheType[T]{
		obj:  value,
		size: size,
	}

	// If the item is not present then create it, else update it
	if get == nil {

		get = &LinkedList[T]{
			key:  key,
			val:  lruCacheTypeObj,
			next: nil,
			prev: nil,
		}

		// Check the capacity. If it is full TODO: decide, we should initiate a deletion procedure
		if this.current+size > this.capacity {

			// Do nothing

			return

		}

		// Add the item

		// Update the size
		this.current += size

		this.cache[key] = get

		// Put the item in first position
		if this.head == nil {
			this.head = get
			this.tail = get

			return
		}

		get.next = this.head
		this.head.prev = get

		this.head = get

		return

	}
	// Else update and put on top of LRU

	get.val = lruCacheTypeObj

	// Update the size
	this.current += size

	// Update the item in the linked list (same stuff as before)

	// If the element is first then do nothing

	if get != this.head {

		// Save next and prev
		next := get.next
		prev := get.prev

		// Remove get from list
		prev.next = next

		// If it is in the middle then fix also reverse
		if next != nil {
			next.prev = prev
		}

		// If the element was the tail fix the tail

		if get == this.tail {
			this.tail = prev
		}

		// get now becomes the head
		get.next = this.head
		get.prev = nil

		// Fix the previous of the head
		this.head.prev = get

		// Attach the get to the head
		this.head = get

	}
}

func (this *LRUCache[T]) GetCurrentSize() uint {
	return this.current
}

func (this *LRUCache[T]) GetTotalSize() uint {
	return this.capacity
}

func (this *LRUCache[T]) GetAvailableSize() uint {
	return this.GetTotalSize() - this.GetCurrentSize()
}

// Potentially useful for debug, but tricky to manage with generics

// func (this *LRUCache) PrintLRU() {

// 	fmt.Println("LRU:")
// 	fmt.Println("head: %d", this.head.key)
// 	fmt.Println("tail: %d", this.tail.key)

// 	fmt.Println("Forwards:")

// 	for i := this.head; i != nil; i = i.next {
// 		fmt.Println("%d", i.key)
// 	}

// 	fmt.Println("Backwards:")

// 	for i := this.tail; i != nil; i = i.prev {
// 		fmt.Println("%d", i.key)
// 	}

//	}
