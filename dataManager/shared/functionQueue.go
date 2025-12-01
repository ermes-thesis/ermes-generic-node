package dataManagerShared

import (
	"sync/atomic"
)

type FunctionQueue interface {
	AddFunction(f func())
	ExecuteFunctions()
	HasFunctions() bool
}

type functionQueue struct {
	functions func()
}

func NewFunctionQueue() FunctionQueue {
	return &functionQueue{
		functions: nil,
	}
}

func (f *functionQueue) AddFunction(function func()) {
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

func (f *functionQueue) ExecuteFunctions() {
	if f.functions != nil {
		f.functions()
		f.functions = nil
	}
}

func (f *functionQueue) HasFunctions() bool {
	return f.functions != nil
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
