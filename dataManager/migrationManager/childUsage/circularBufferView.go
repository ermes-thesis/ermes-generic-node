package childUsage

type CircularBuffer struct {
	Data     []*childResourceUsage
	Capacity int
	Head     int
}

func NewCircularBuffer(capacity int) *CircularBuffer {
	return &CircularBuffer{
		Data:     make([]*childResourceUsage, capacity),
		Capacity: capacity,
		Head:     0,
	}
}

func (cb *CircularBuffer) Add(data *childResourceUsage) {
	cb.Data[cb.Head] = data
}

func (cb *CircularBuffer) Movehead() {
	cb.Head = (cb.Head + 1) % cb.Capacity
	cb.Data[cb.Head] = NewChildResourceUsage() //avoid nil values
}
