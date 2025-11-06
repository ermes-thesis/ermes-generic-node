package childUsage

type CircularBuffer struct {
	data []*ChildResourceUsage
	head int
}

func NewCircularBuffer(capacity int) *CircularBuffer {
	cb := CircularBuffer{
		data: make([]*ChildResourceUsage, capacity),
		head: 0,
	}
	cb.data[cb.head] = NewChildResourceUsage()
	return &cb
}

func (cb *CircularBuffer) MoveHead() {
	cb.head = (cb.head + 1) % len(cb.data)
	cb.data[cb.head] = NewChildResourceUsage() //avoid nil values
}

func (cb *CircularBuffer) Head() *ChildResourceUsage {
	return cb.data[cb.head]
}

func (cb *CircularBuffer) HeadCursor() int {
	return cb.head
}

func (cb *CircularBuffer) Get(i int) *ChildResourceUsage {
	return cb.data[(cb.head-i+cb.Capacity())%cb.Capacity()]
}

func (cb *CircularBuffer) Capacity() int {
	return len(cb.data)
}

func (cb *CircularBuffer) ResetRead() {
	for _, d := range cb.data {
		if d != nil {
			d.Read = ResourceUsage{}
		}
	}
}
