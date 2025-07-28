package migrationManager

type CircularBuffer struct {
	Data     []*childResourceUsage
	capacity int
	Head     int
}

func NewCircularBuffer(capacity int) *CircularBuffer {
	return &CircularBuffer{
		Data:     make([]*childResourceUsage, capacity),
		capacity: capacity,
		Head:     0,
	}
}

func (cb *CircularBuffer) Add(data *childResourceUsage) {
	cb.Data[cb.Head] = data
}

func (cb *CircularBuffer) Movehead() {
	cb.Head = (cb.Head + 1) % cb.capacity
	cb.Data[cb.Head] = NewChildResourceUsage() //avoid nil values
}
