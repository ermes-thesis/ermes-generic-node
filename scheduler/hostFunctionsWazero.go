package scheduler

import (
	"context"
	"fmt"

	"github.com/ermes-thesis/ermes-common/wasm-pool"
	"github.com/ermes-thesis/ermes-common/wasm-pool/executor/adapter"
	"github.com/tetratelabs/wazero/api"
)

func (h *HostFunctions) RegisterWazeroHostFns(pool *wasm.WazeroExecutorPool) error {
	h.logger.Info("Starting registration of Wazero host functions")

	fns := map[string]any{
		"hostGet": func(ctx context.Context, m api.Module, collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, userIdPtr, userIdLength int32) int32 {
			return h.hostGet(createWazeroAdapter(m), collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, userIdPtr, userIdLength)
		},
		"hostMultipleGet": func(ctx context.Context, m api.Module, collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength, userIdPtr, userIdLength int32) int32 {
			return h.hostMultipleGet(createWazeroAdapter(m), collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength, userIdPtr, userIdLength)
		},
		"hostCreateCollection": func(ctx context.Context, m api.Module, metadataPointer, metadataLength, skipUniquenessCheck int32) int32 {
			return h.hostCreateCollection(createWazeroAdapter(m), metadataPointer, metadataLength, skipUniquenessCheck)
		},
	}

	err := pool.RegisterHostFns(fns)
	if err != nil {
		h.logger.Error("Error registering Wazero host functions", "host_functions_map", fmt.Sprintf("%+v", fns), "error", err)
		return fmt.Errorf("failed to register module: %v", err)
	}
	h.logger.Info("Registration of Wazero host functions completed successfully")
	return nil
}

func createWazeroAdapter(m api.Module) *adapter.WazeroAdapter {
	return &adapter.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
	}
}
