package dataManagerShared

import (
	"sync/atomic"
)

type FunctionQueue interface {
	AddFunction(f func())
	ExecuteFunctions()
	HasFunctions() bool
	PopFunction() func()
}

type functionQueue struct {
	functions []func()
}

func NewFunctionQueue() FunctionQueue {
	return &functionQueue{
		functions: make([]func(), 0),
	}
}

func (f *functionQueue) AddFunction(function func()) {
	f.functions = append(f.functions, function)
}

func (f *functionQueue) ExecuteFunctions() {
	for _, function := range f.functions {
		function()
	}
	f.functions = make([]func(), 0)
}

func (f *functionQueue) HasFunctions() bool {
	return len(f.functions) > 0
}

func (f *functionQueue) PopFunction() func() {
	fn := f.functions[len(f.functions)-1]
	if len(f.functions) > 0 {
		f.functions = f.functions[:len(f.functions)-1]
	}
	return fn
}

type FunctionQueueWithCounter struct {
	counter atomic.Uint32
	//mu        sync.Mutex //Not necessary in our case, all functions are added before starting the goroutines that consume functions
	functions func()
}

func NewFunctionQueueWithCounter() FunctionQueue {
	return &FunctionQueueWithCounter{
		counter:   atomic.Uint32{},
		functions: nil,
	}
}

func (f *FunctionQueueWithCounter) SetCounter(counter uint32) {
	f.counter.Store(counter)
}

func (f *FunctionQueueWithCounter) AddFunction(function func()) {
	oldFunctions := f.functions
	if oldFunctions == nil {
		f.functions = function
	} else {
		f.functions = func() {
			oldFunctions()
			function()
		}
	}
}

func (f *FunctionQueueWithCounter) ExecuteFunctions() {
	val := f.counter.Load()
	if f.counter.Load() > 0 {
		val = f.counter.Add(^uint32(0)) // Decrement by 1
	}
	if f.functions != nil && val == 0 { // atomic condition???
		f.functions()
		f.functions = nil
	}
}

func (f *FunctionQueueWithCounter) HasFunctions() bool {
	return f.functions != nil
}

func (f *FunctionQueueWithCounter) PopFunction() func() {
	// Not implemented
	return nil
}
