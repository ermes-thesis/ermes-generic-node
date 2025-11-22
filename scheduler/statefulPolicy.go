package scheduler

import (
	"bytes"
	"dataManager/collectionsDiscoverer"
	shared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"server"
	"strconv"
	"time"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
)

type statefulPolicy struct {
	*baseScheduler
}

func newStatefulPolicy(sched *baseScheduler) Scheduler {
	return &statefulPolicy{
		baseScheduler: sched,
	}
}

func (s *statefulPolicy) RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder) error {
	err := server.AddCustomRoute("/pvt-offload-function/{name}/{version}", true, s.handleOffloadFunctionInvocation(tokenDecoder))
	if err != nil {
		s.log.Error("Failed to register custom routes", "path", "/pvt-offload-function/{name}/{version}", "error", err)
		return fmt.Errorf("Error while registering /pvt-offload-function/{name}/{version}: %v\n", err)
	}
	err = server.AddCustomRoute("/pvt-offload-function/{name}", true, s.handleOffloadFunctionInvocation(tokenDecoder))
	if err != nil {
		s.log.Error("Failed to register custom routes", "path", "/pvt-offload-function/{name}", "error", err)
		return fmt.Errorf("Error while registering /pvt-offload-function/{name}: %v\n", err)
	}

	return nil
}

func (s *statefulPolicy) retryIsCollectionLocallyPresent(colId string, seqNum uint64) (bool, shared.Collection, error) {
	var (
		found      bool
		collection shared.Collection
		err        error
	)

	backoff := s.cfg.StartingOutdatedDataBackoff

	for i := 0; i < s.cfg.MaxOutdatedDataRetries; i++ {
		found, collection, err = s.dataManager.GetCollectionsDiscoverer().IsCollectionLocallyPresent(colId, seqNum)
		var toMigrateErr *collectionsDiscoverer.ToMigrateError
		if err == nil {
			if collection != nil {
				s.log.Info("Collection existence known locally", "collection_id", colId, "seq_num", seqNum, "collection", fmt.Sprintf("%+v", collection))
			} else {
				s.log.Info("Collection found locally", "collection_id", colId, "seq_num", seqNum, "is_locally_present", found)
			}
			return found, collection, nil
		} else if errors.As(err, &toMigrateErr) {
			return false, nil, toMigrateErr
		}
		s.log.Debug("Retry collection presence check", "collection_id", colId, "seq_num", seqNum, "current_attempt", i, "error", err)
		if i < s.cfg.MaxOutdatedDataRetries-1 {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > s.cfg.MaxOutdatedDataBackoff {
				backoff = s.cfg.MaxOutdatedDataBackoff
			}
		}
	}
	s.log.Warn("Sequential collection has not been updated within the time/retries limit", "collection_id", colId, "seq_num", seqNum)
	return found, collection, fmt.Errorf("couldn't access an updated version of the collection after %d retries: %v", s.cfg.MaxOutdatedDataRetries, err)
}

func (s *statefulPolicy) Schedule(scheduleRequest ScheduleRequest, reqHostname string, groups []string, userId string) (*FunctionResult, uint64, error) {
	s.log.Info("Scheduling function", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
	s.log.Debug("Scheduling function", "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", string(scheduleRequest.Payload), "sequential_seq_num", scheduleRequest.SequentialSeqNumber, "req_hostname", reqHostname)

	if len(scheduleRequest.Collections) == 0 {
		s.log.Info("No collections specified, executing locally", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
		s.log.Debug("No collections specified, executing locally", "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", string(scheduleRequest.Payload))
		return s.executeLocally(scheduleRequest, reqHostname, groups, userId, "", nil, false)
	}

	s.log.Info("Start to analyze collections for scheduling decision", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId)
	// Initializing vars for function returned collections analysis
	var sequentialCollection *shared.StrongCollection
	sequentialCollectionNeeded := false
	hasEventualLocal := false
	eventualHostCounts := make(map[string]uint)
	maxEventualCount := uint(0)
	hostWithMoreEventual := ""
	if s.serverInfo.Father != nil {
		hostWithMoreEventual = s.serverInfo.Father.HostName
	}

	// Cycling over all the returned collections (they are grouped based on their "AS" defined by the dev)
	for _, cols := range scheduleRequest.Collections {
		// Cycling over all the collections with the same "AS"
		for _, col := range cols {
			// Checking if the collection is locally present, the check is retried maximum 10 times, every 500ms if the
			// collection is a sequential consistent one. (In case of an eventual consistent the seqNumber passed is ignored)
			found, collection, err := s.retryIsCollectionLocallyPresent(col.Id, scheduleRequest.SequentialSeqNumber)
			var toMigrateErr *collectionsDiscoverer.ToMigrateError
			if err != nil {
				if errors.As(err, &toMigrateErr) {
					s.log.Error("Collection is being migrated, cannot schedule function", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collection_id", col.Id, "seq_num", scheduleRequest.SequentialSeqNumber, "error", err)
					return &FunctionResult{
						StatusCode: 423,
						Hostname:   "",
						Body:       nil,
					}, 0, toMigrateErr
				}
			}

			if col.Metadata.Consistency == model.SequentialConsistent {
				sequentialCollectionNeeded = true
				if sequentialCollection != nil {
					s.log.Error("Multiple sequential collections requested for execution, which is not supported.", "sequential_collection_needed", sequentialCollectionNeeded, "already_found_seq_collection", fmt.Sprintf("%+v", sequentialCollection), "just_found_seq_collection", fmt.Sprintf("%+v", col), "has_eventual_local", hasEventualLocal, "eventual_host_counts", eventualHostCounts, "max_eventual_count", maxEventualCount, "host_with_more_eventual", hostWithMoreEventual, "error", err)
					return nil, 0, fmt.Errorf("multiple sequentially consistent collections are not supported")
				}
				// If collection is NIL, it means that the current node does not know its existence
				if collection != nil {
					sequentialCollection = collection.(*shared.StrongCollection)
				}
			} else {
				if found {
					hasEventualLocal = true
				}

				// If the currently iterated collection is known by the current node, we count the host for this collection
				if collection != nil {
					for hostId := range collection.HostsId() {
						eventualHostCounts[hostId]++
						if eventualHostCounts[hostId] > maxEventualCount {
							maxEventualCount = eventualHostCounts[hostId]
							hostWithMoreEventual = hostId
						}
					}
				} else if s.serverInfo.Father != nil {
					hostId := s.serverInfo.Father.HostName
					eventualHostCounts[hostId]++
					if eventualHostCounts[hostId] > maxEventualCount {
						maxEventualCount = eventualHostCounts[hostId]
						hostWithMoreEventual = hostId
					}
				}
			}
		}
	}

	s.log.Info("Collection analysis completed", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "sequential_coll_needed", sequentialCollectionNeeded, "sequential_collection_found", fmt.Sprintf("%+v", sequentialCollection), "has_eventual_local", hasEventualLocal, "eventual_host_counts", fmt.Sprintf("%+v", eventualHostCounts), "max_eventual_count", maxEventualCount, "host_with_more_eventual", hostWithMoreEventual)
	// Route based on collection analysis
	// Priority: Sequential > Local Eventual > Remote Eventual
	if sequentialCollectionNeeded {
		s.log.Debug("Sequential collection requested for execution. Scheduling accordingly", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", scheduleRequest.Payload, "sequential_seq_num", scheduleRequest.SequentialSeqNumber, "req_hostname", reqHostname)

		if sequentialCollection == nil {
			if s.serverInfo.Father == nil {
				s.log.Error("Couldn't find in the architecture the requested sequential collection", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", scheduleRequest.Payload, "sequential_seq_num", scheduleRequest.SequentialSeqNumber, "req_hostname", reqHostname)
				return nil, 0, errors.New("sequential collection requested does not exists")
			}
			s.log.Info("Sequential collection existence not found. Trying to forward the request to father node",
				"fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, userId, "forwarded_to", s.serverInfo.Father.HostName)
			return s.forwardRequest(s.serverInfo.Father.HostName, scheduleRequest)
		}

		functionMetadata, err := s.cache.GetMetadata(scheduleRequest.FunctionName, scheduleRequest.FunctionVersion)
		if err != nil {
			s.log.Error("Failed to retrieve function metadata", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", scheduleRequest.Payload, "sequential_seq_num", scheduleRequest.SequentialSeqNumber, "req_hostname", reqHostname, "error", err)
			return nil, 0, fmt.Errorf("error retrieving function metadata: %v", err)
		}

		if sequentialCollection.IsLocal() {
			// Re-check if the collection is still local, after acquiring the lock
			// This is needed because the collection could be migrated while we were waiting for the lock
			// If the collection is not local anymore, we need to forward the request
			//if !sequentialCollection.IsLocal() {
			//	return s.forwardRequestWithSequential(functionName, functionVersion, collections, groupToken, sequentialSeqNumber, functionMetadata.StrongCollWriteAccess, sequentialCollection)
			//}

			// If Sequential collection is present, and it's the leader execute locally.
			// If Sequential collection is present, and the function needs only to read it, execute locally
			if sequentialCollection.IsLeader || !functionMetadata.StrongCollWriteAccess {
				if functionMetadata.StrongCollWriteAccess {
					sequentialCollection.WriteMutex.Lock()
					defer sequentialCollection.WriteMutex.Unlock()
				}
				s.log.Info("Executing function locally with sequential collection", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
				s.log.Debug("Executing function locally with sequential collection", "seq_collection_id", sequentialCollection.CollectionId(), "sequential_seq_num", scheduleRequest.SequentialSeqNumber, "is_leader", sequentialCollection.IsLeader, "write_access_requested", functionMetadata.StrongCollWriteAccess)
				return s.executeLocally(scheduleRequest, reqHostname, groups, userId, sequentialCollection.CollectionId(), &sequentialCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock, functionMetadata.StrongCollWriteAccess)
			}

			s.log.Info("Sequential collection must be accessed in write but the local sequential collection is not the leader. Forwarding request to the host containing the leader sequential collection",
				"fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "forwarded_to", sequentialCollection.LeaderId)
			// In all the other cases forward to the leader replica of the sequential collection
			dest := sequentialCollection.LeaderId
			if dest == "" && s.serverInfo.Father != nil {
				dest = s.serverInfo.Father.HostName
			} else if dest == "" && s.serverInfo.Father == nil {
				return nil, 0, fmt.Errorf("sequential collection leader not found for collection %s", sequentialCollection.CollectionId())
			}

			sequentialCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RUnlock()
			s.dataManager.GetMigrationManager().HandlePostponedDecisions(sequentialCollection)
			return s.forwardRequest(dest, scheduleRequest)
		}

		s.log.Info("Sequential collection found, but we are in a parent node since it is not local. Forwarding to the correct child node.", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
		return s.forwardRequestWithSequential(scheduleRequest, sequentialCollection, functionMetadata.StrongCollWriteAccess)
	}

	// If is not requested a Sequential Collection, and the current node has at least one of the eventual collections
	// requested, we execute the function locally
	if hasEventualLocal {
		s.log.Info("Executing function locally with eventual", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId)
		s.log.Debug("Executing function locally with eventual", "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "with_request_body", string(scheduleRequest.Payload), "user_access_groups", groups, "request_hostname", reqHostname)
		return s.executeLocally(scheduleRequest, reqHostname, groups, userId, "", nil, false)
	}

	// We have only Eventual Collection requested, and no one of them is local, we forward the scheduling request
	// towards the DIRECTION where we have more Eventual Collections
	s.log.Info("Only eventual collections detected. None is local, forwarding request to host with more eventual collections detected", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "forwarded_to", hostWithMoreEventual)
	return s.forwardRequest(hostWithMoreEventual, scheduleRequest)
}

func (s *statefulPolicy) forwardRequestWithSequential(scheduleRequest ScheduleRequest, sequentialCollection *shared.StrongCollection, writeAccess bool) (*FunctionResult, uint64, error) {
	// If the Sequential collection is not locally present, and the function needs to access it in write. Forward towards
	// the leader.
	if writeAccess {
		s.log.Debug("Forwarding function scheduling to the sequential collection leader since we have to access it in write", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "forward_to", sequentialCollection.LeaderId)
		dest := sequentialCollection.LeaderId
		if dest == "" && s.serverInfo.Father != nil {
			dest = s.serverInfo.Father.HostName
		} else if dest == "" && s.serverInfo.Father == nil {
			return nil, 0, fmt.Errorf("sequential collection leader not found for collection %s", sequentialCollection.CollectionId())
		}
		return s.forwardRequest(dest, scheduleRequest)
	}

	// If the Sequential collection is not locally present, and the function require only reading it. Forward towards to
	// one of the available replicas
	minLatency := math.MaxInt
	var hostWithMinLatency string
	for hostId := range sequentialCollection.HostsId() {
		currLatency := s.serverInfo.GetLatencyFor(hostId)
		s.log.Debug("Checked latency for host", "host_id", hostId, "curr_latency", currLatency, "min_latency", minLatency)
		if currLatency < minLatency {
			hostWithMinLatency = hostId
			minLatency = currLatency
		}
	}

	s.log.Debug("Replica selection for sequential collection read", "host_with_min_latency", hostWithMinLatency, "min_latency", minLatency)
	if hostWithMinLatency != "" {
		s.log.Debug("Forwarding function scheduling to one of the available sequential collection replicas, based on latency, since we have to access it in read", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "forward_to", hostWithMinLatency)
		return s.forwardRequest(hostWithMinLatency, scheduleRequest)
	}

	// If "HostId" is empty we fall back to the father (shouldn't happen, since if we are in this branch, it means that
	// the node knows the existence of the collection, therefore the collection should be local to the node, or in
	// one of its children, meaning that "HostId" should NOT be empty)
	if s.serverInfo.Father != nil {
		s.log.Debug("Forwarding function scheduling to father as fallback", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "forward_to", s.serverInfo.Father.HostName)
		return s.forwardRequest(s.serverInfo.Father.HostName, scheduleRequest)
	}

	// If we are here, it means that something wrong has happened in the scheduling process, and we are in the Cloud Node
	s.log.Error("No available host for sequential collection (unexpected)", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "collection_id", sequentialCollection.CollectionId())
	return nil, 0, errors.New("no available host for sequential collection")
}

func (s *statefulPolicy) forwardRequest(dest string, scheduleRequest ScheduleRequest) (*FunctionResult, uint64, error) {
	s.log.Debug("Preparing request to offload scheduling", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "destination", dest, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "sequential_coll_seq_num", scheduleRequest.SequentialSeqNumber, "cumulative_latency", scheduleRequest.CumulativeLatency)

	url := fmt.Sprintf("http://%s/pvt-offload-function/%s/%s", dest, scheduleRequest.FunctionName, scheduleRequest.FunctionVersion)
	req, err := http.NewRequest(scheduleRequest.HTTPMethod, url, bytes.NewReader(scheduleRequest.Payload))
	if err != nil {
		s.log.Error("Error creating offloading request", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "sequential_coll_seq_num", scheduleRequest.SequentialSeqNumber, "cumulative_latency", scheduleRequest.CumulativeLatency, "error", err)
		return nil, 0, fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Set("X-ERMES-GROUPS", scheduleRequest.GroupToken)
	collectionsJson, err := json.Marshal(scheduleRequest.Collections)
	if err != nil {
		s.log.Error("Error marshalling collections for offloading request", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "sequential_coll_seq_num", scheduleRequest.SequentialSeqNumber, "cumulative_latency", scheduleRequest.CumulativeLatency, "error", err)
		return nil, 0, fmt.Errorf("error marshalling collections: %v", err)
	}
	req.Header.Set("X-ERMES-COLLECTIONS", string(collectionsJson))
	req.Header.Set("X-ERMES-SECRET", s.serverInfo.Secret)
	req.Header.Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", scheduleRequest.SequentialSeqNumber))
	req.Header.Set("X-ERMES-HOSTNAME", s.serverInfo.HostName)
	req.Header.Set("X-ERMES-LATENCY", strconv.Itoa(scheduleRequest.CumulativeLatency+s.serverInfo.GetLatencyFor(dest)))
	req.Header.Set("X-ERMES-REDIRECTS", strconv.Itoa(scheduleRequest.NumRedirects+1))

	s.log.Debug("Forward request prepared with headers", "headers", fmt.Sprintf("%v", req.Header))
	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		s.log.Error("Error forwarding request", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "sequential_coll_seq_num", scheduleRequest.SequentialSeqNumber, "cumulative_latency", scheduleRequest.CumulativeLatency, "error", err)
		return nil, 0, fmt.Errorf("error forwarding request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		s.log.Error("Received error response from offloaded scheduling", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "sequential_coll_seq_num", scheduleRequest.SequentialSeqNumber, "cumulative_latency", scheduleRequest.CumulativeLatency, "status_code", resp.StatusCode)
		return &FunctionResult{
			StatusCode: resp.StatusCode,
			Hostname:   resp.Header.Get("X-ERMES-EXECUTION-HOST"),
			Body:       nil,
		}, 0, fmt.Errorf("error response from offloaded scheduling: status code %d", resp.StatusCode)
	}

	sequentialSeqNumberStr := resp.Header.Get("X-ERMES-SEQUENTIAL-NUMBER")
	var sequentialSeqNumber uint64
	if sequentialSeqNumberStr != "" {
		_, err = fmt.Sscanf(sequentialSeqNumberStr, "%d", &sequentialSeqNumber)
		if err != nil {
			s.log.Error("Error parsing sequential sequence number", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "sequential_coll_seq_num", scheduleRequest.SequentialSeqNumber, "cumulative_latency", scheduleRequest.CumulativeLatency, "error", err)
			return nil, 0, fmt.Errorf("error parsing sequential sequence number: %v", err)
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.log.Error("Error reading response", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", scheduleRequest.Collections), "sequential_coll_seq_num", scheduleRequest.SequentialSeqNumber, "cumulative_latency", scheduleRequest.CumulativeLatency, "error", err)
		return nil, 0, fmt.Errorf("error reading response: %v", err)
	}

	fnRes := FunctionResult{
		StatusCode: resp.StatusCode,
		Hostname:   resp.Header.Get("X-ERMES-EXECUTION-HOST"),
		Body:       body,
	}
	s.log.Debug("Received response from offloaded scheduling", "fn_response", fmt.Sprintf("%+v", fnRes), "new_sequential_number", sequentialSeqNumber, "old_sequential_number", scheduleRequest.SequentialSeqNumber)
	return &fnRes, sequentialSeqNumber, nil
}
