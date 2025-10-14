package scheduler

import (
	"context"
	"fmt"

	"github.com/ermes-thesis/ermes-common/wasm-pool"
	"github.com/ermes-thesis/ermes-common/wasm-pool/executor/adapter"
	"github.com/tetratelabs/wazero/api"
)

func (h *HostFunctions) RegisterWazeroHostFns(pool wasm.ExecutorPool) error {
	fns := map[string]any{
		"hostGet": func(ctx context.Context, m api.Module, collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength int32) int32 {
			return h.hostGet(createWazeroAdapter(m), collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength)
		},
		"hostMultipleGet": func(ctx context.Context, m api.Module, collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength int32) int32 {
			return h.hostMultipleGet(createWazeroAdapter(m), collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength)
		},
		"hostCreateCollection": func(ctx context.Context, m api.Module, metadataPointer, metadataLength int32) int32 {
			return h.hostCreateCollection(createWazeroAdapter(m), metadataPointer, metadataLength)
		},
	}

	err := pool.RegisterHostFns(fns)
	if err != nil {
		return fmt.Errorf("failed to register module: %v", err)
	}
	return nil
}

func createWazeroAdapter(m api.Module) *adapter.WazeroAdapter {
	return &adapter.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
	}
}
