package callbackStack

import "sync"

type CallStack struct {
	stack []func()
	mu    sync.Mutex
}

func NewCallStack() *CallStack {
	return &CallStack{
		stack: make([]func(), 0),
	}
}

func (s *CallStack) Push(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stack = append(s.stack, f)
}

func (s *CallStack) Pop() func() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.stack) == 0 {
		return nil
	}

	last := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	return last
}

func (s *CallStack) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.stack)
}

func (s *CallStack) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.stack) == 0
}
