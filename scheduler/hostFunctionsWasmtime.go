package scheduler

import (
	"fmt"

	"github.com/bytecodealliance/wasmtime-go/v37"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
	"github.com/ermes-thesis/ermes-common/wasm-pool/executor/adapter"
)

func (h *HostFunctions) RegisterWasmtimeHostFns(pool *wasm.WasmtimeExecutorPool) error {
	fns := map[string]any{
		"hostGet": func(collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, userIdPtr, userIdLength int32, caller *wasmtime.Caller) int32 {
			return h.hostGet(createWasmtimeAdapter(caller), collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, userIdPtr, userIdLength)
		},
		"hostMultipleGet": func(collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength, userIdPtr, userIdLength int32, caller *wasmtime.Caller) int32 {
			return h.hostMultipleGet(createWasmtimeAdapter(caller), collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength, userIdPtr, userIdLength)
		},
		"hostCreateCollection": func(metadataPointer int32, metadataLength int32, caller *wasmtime.Caller) int32 {
			return h.hostCreateCollection(createWasmtimeAdapter(caller), metadataPointer, metadataLength)
		},
	}

	err := pool.RegisterHostFns(fns)
	if err != nil {
		return fmt.Errorf("failed to register module: %v", err)
	}
	return nil
}

func createWasmtimeAdapter(caller *wasmtime.Caller) *adapter.WasmtimeAdapter {
	return &adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}
}
