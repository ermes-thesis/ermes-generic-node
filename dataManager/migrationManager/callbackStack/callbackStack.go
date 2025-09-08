package callbackStack

type CallStack []func()

func NewCallStack() CallStack {
	return make(CallStack, 0)
}

func (s *CallStack) Push(f func()) {
	*s = append(*s, f)
}

func (s *CallStack) Pop() func() {
	if len(*s) == 0 {
		return nil
	}
	last := (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
	return last
}
