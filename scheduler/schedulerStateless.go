package scheduler

import (
	"caches"
	"config"
	datamanager "dataManager"
	"fmt"
	"metadataManager"
	"server"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
)

type SchedulerStateless struct {
	*baseScheduler
}

func NewSchedulerStateless(dataManager datamanager.DataManager, metadataManager metadataManager.MetadataManager, cache caches.FunctionCache, wasmExecutorPool wasm.ExecutorPool, serverInfo *server.ServerInfo, config config.SchedulerConfig, logger logger.Logger) Scheduler {
	return &SchedulerStateless{
		baseScheduler: &baseScheduler{
			dataManager:      dataManager,
			metadataManager:  metadataManager,
			cache:            cache,
			wasmExecutorPool: wasmExecutorPool,
			serverInfo:       serverInfo,
			cfg:              config,
			log:              logger,
		},
	}
}

func (s *SchedulerStateless) RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder, metadataManager metadataManager.MetadataManager) error {
	return nil
}

func (s *SchedulerStateless) Schedule(scheduleRequest ScheduleRequest, reqHostname string, groups []string, userId string) (*FunctionResult, uint64, error) {
	s.log.Info("Scheduling function", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
	s.log.Debug("Scheduling function", "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", string(scheduleRequest.Payload), "sequential_seq_num", scheduleRequest.SequentialSeqNumber, "req_hostname", reqHostname)

	return s.executeLocally(scheduleRequest, reqHostname, groups, userId, "", false)
}
