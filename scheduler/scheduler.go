package scheduler

import (
	"fmt"
	"metadataManager/utils"
)

type Scheduler interface {
	Schedule(functionName string, functionVersion string, collections map[string][]utils.CollectionInfo) (int, []byte, error)
}

type SchedulerImpl struct {
}

func NewScheduler() Scheduler {
	return &SchedulerImpl{}
}

func (s *SchedulerImpl) Schedule(functionName string, functionVersion string, collections map[string][]utils.CollectionInfo) (int, []byte, error) {
	fmt.Println("Scheduling function:", functionName, "version:", functionVersion)
	fmt.Printf("With collections: %v\n", collections)
	return 200, []byte(`{"message": "Function scheduled successfully"}`), nil
}
