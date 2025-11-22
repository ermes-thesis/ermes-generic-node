package scheduler

import (
	"fmt"

	"github.com/ermes-thesis/ermes-common/wasm-pool"
	"github.com/ermes-thesis/ermes-common/wasm-pool/adapter"
	"github.com/second-state/WasmEdge-go/wasmedge"
)

func (h *HostFunctions) RegisterWasmedgeHostFns(pool *wasm.WasmedgeExecutorPool) error {
	h.logger.Info("Starting registration of Wasmedge host functions")
	// It's called two params but the 2nd parm is only a single Int32 since it is a boolean value, therefore we
	// don't need to pass also its length
	fnTwoParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	fnFourParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	// Create a function context with the function type and host function body.
	// The third parameter is the pointer to the additional data.
	// Developers should guarantee the life cycle of the data, and it can be
	// `nil` if the external data is not needed.
	// The last parameter can be 0 if developers do not need the cost measuring.
	fns := map[string]any{
		"hostGet": wasmedge.NewFunction(fnFourParamOneReturnType, func(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
			return createWasmedgeResult(h.hostGet(createWasmedgeAdapter(callframe), params[0].(int32), params[1].(int32), params[2].(int32), params[3].(int32), params[4].(int32), params[5].(int32), params[6].(int32), params[7].(int32)))
		}, nil, 0),
		"hostMultipleGet": wasmedge.NewFunction(fnFourParamOneReturnType, func(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
			return createWasmedgeResult(h.hostMultipleGet(createWasmedgeAdapter(callframe), params[0].(int32), params[1].(int32), params[2].(int32), params[3].(int32), params[4].(int32), params[5].(int32), params[6].(int32), params[7].(int32)))
		}, nil, 0),
		"hostCreateCollection": wasmedge.NewFunction(fnTwoParamOneReturnType, func(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
			return createWasmedgeResult(h.hostCreateCollection(createWasmedgeAdapter(callframe), params[0].(int32), params[1].(int32), params[2].(int32)))
		}, nil, 0),
	}
	// If the function object is not added into a module instance object, it should be released.
	fnTwoParamOneReturnType.Release()
	fnFourParamOneReturnType.Release()

	err := pool.RegisterHostFns(fns)
	if err != nil {
		h.logger.Error("Error registering Wasmedge host functions", "host_functions_map", fmt.Sprintf("%+v", fns), "error", err)
		return fmt.Errorf("failed to register module: %v", err)
	}
	h.logger.Info("Registration of Wasmedge host functions completed successfully")
	return nil
}

func createWasmedgeAdapter(callframe *wasmedge.CallingFrame) *adapter.WasmedgeAdapter {
	return &adapter.WasmedgeAdapter{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}
}

func createWasmedgeResult(res int32) ([]any, wasmedge.Result) {
	returns := make([]any, 1)
	returns[0] = res
	return returns, wasmedge.Result_Success
}
