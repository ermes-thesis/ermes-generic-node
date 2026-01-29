package scheduler

import (
	"fmt"
	"server"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
)

type statelessPolicy struct {
	*baseScheduler
}

func newStatelessPolicy(sched *baseScheduler) Scheduler {
	return &statelessPolicy{
		baseScheduler: sched,
	}
}

func (s *statelessPolicy) RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder) error {
	return nil
}

func (s *statelessPolicy) Schedule(scheduleRequest ScheduleRequest, reqHostname string, groups []string, userId string) (*FunctionResult, uint64, error) {
	s.log.Info("Scheduling function", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
	if s.log.Enabled(logger.DebugLevel) {
		s.log.Debug("Scheduling function", "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", string(scheduleRequest.Payload), "sequential_seq_num", scheduleRequest.SequentialSeqNumber, "req_hostname", reqHostname)
	}

	sequentialCollectionId := ""
	for _, colls := range scheduleRequest.Collections {
		for _, coll := range colls {
			if coll.Metadata.Consistency == model.SequentialConsistent {
				if sequentialCollectionId != "" {
					return nil, 0, fmt.Errorf("stateless scheduler supports at most one sequentially consistent collection per function execution request, found multiple: %s and %s", sequentialCollectionId, coll.Id)
				}
				sequentialCollectionId = coll.Id
			}
		}
	}

	functionMetadata, err := s.cache.GetMetadata(scheduleRequest.FunctionName, scheduleRequest.FunctionVersion)
	if err != nil {
		s.log.Error("Failed to retrieve function metadata", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", scheduleRequest.Payload, "sequential_seq_num", scheduleRequest.SequentialSeqNumber, "req_hostname", reqHostname, "error", err)
		return nil, 0, fmt.Errorf("error retrieving function metadata: %v", err)
	}

	return s.executeLocally(scheduleRequest, reqHostname, groups, userId, sequentialCollectionId, nil, functionMetadata.StrongCollWriteAccess, nil)
}
