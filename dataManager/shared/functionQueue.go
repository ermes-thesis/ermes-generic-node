package dataManagerShared

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
