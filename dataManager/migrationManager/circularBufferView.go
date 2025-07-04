package migrationManager

import (
	shared "dataManager/shared"
)

type CircularBuffer struct {
	Data     []shared.Collection
	capacity int
	Head     int
}

func NewCircularBuffer(capacity int) *CircularBuffer {
	return &CircularBuffer{
		Data:     make([]shared.Collection, capacity),
		capacity: capacity,
		Head:     0,
	}
}

func (cb *CircularBuffer) Add(collection shared.Collection) {
	cb.Data[cb.Head] = collection
	cb.Head = (cb.Head + 1) % cb.capacity
}
