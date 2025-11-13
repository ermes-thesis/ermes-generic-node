package scheduler

import (
	"fmt"
	"server"
	"tokenDecoder"
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
	s.log.Debug("Scheduling function", "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", string(scheduleRequest.Payload), "sequential_seq_num", scheduleRequest.SequentialSeqNumber, "req_hostname", reqHostname)

	return s.executeLocally(scheduleRequest, reqHostname, groups, userId, "", nil, false)
}
