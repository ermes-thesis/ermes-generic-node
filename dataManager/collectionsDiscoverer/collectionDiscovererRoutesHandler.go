package collectionsDiscoverer

import (
	gossip "dataManager/messages"
	"dataManager/migrationManager"
	shared "dataManager/shared"
	"encoding/json"
	"io"
	log "logger"
	"net/http"
)

func (c *CollectionsDiscovererImp) handleIncomingRemoteAccess(logger log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Println("HTTP-REQ", "New remote access request received through HTTP request", log.INFO)

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Println("HTTP-REQ", "Error reading request body", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var msg gossip.RemoteAccessRequest
		if err := json.Unmarshal(bodyBytes, &msg); err != nil {
			logger.Println("HTTP-REQ", "Error parsing remote access request", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var res interface{}
		var dbErr error

		if len(msg.RecordNames) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		} else if len(msg.RecordNames) == 1 {
			dbFn := func() (interface{}, error) {
				return c.dbInterface.GetRecord(msg.CollectionId, msg.RecordNames[0])
			}
			remoteFn := func(addr string) (interface{}, error) {
				return c.WeakCollectionRemoteRecordAccess(addr, msg.CollectionId, msg.RecordNames[0])
			}
			res, dbErr = AccessRecords(c, msg.CollectionId, dbFn, remoteFn)
		} else {
			dbFn := func() (interface{}, error) {
				return c.dbInterface.GetRecords(msg.CollectionId, msg.RecordNames...)
			}
			remoteFn := func(addr string) (interface{}, error) {
				return c.WeakCollectionRemoteRecordsAccess(addr, msg.CollectionId, msg.RecordNames...)
			}
			res, dbErr = AccessRecords(c, msg.CollectionId, dbFn, remoteFn)
		}

		resp := gossip.RemoteAccessResponse{
			Records: res.([]any),
		}

		jsonResp, jsonErr := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")

		if dbErr != nil || jsonErr != nil {
			logger.Println("HTTP-REQ", "Error accessing collection: "+dbErr.Error(), log.ERROR)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Write(jsonResp)

		w.WriteHeader(http.StatusOK)
	})
}

func (c *CollectionsDiscovererImp) handleIncomingUpdatePropagation(logger log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Println("HTTP-REQ", "New update propagation request received through HTTP request", log.INFO)

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Println("HTTP-REQ", "Error reading request body", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var msg gossip.CollectionUpdateMessages
		if err := json.Unmarshal(bodyBytes, &msg); err != nil {
			logger.Println("HTTP-REQ", "Error parsing remote access request", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK) //response

		c.UpdateCollectionsRecorsAndPropogate(msg, false, msg[0].SenderIp)
	})
}

func (c *CollectionsDiscovererImp) handleIncomingNewCollection(logger log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Println("HTTP-REQ", "New collection creation request received through HTTP request", log.INFO)

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Println("HTTP-REQ", "Error reading request body", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var msg gossip.NewCollectionMessage
		if err := json.Unmarshal(bodyBytes, &msg); err != nil {
			logger.Println("HTTP-REQ", "Error parsing new collection message", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		collectionId := msg.CollectionId
		isStrong := msg.IsStrong

		//sync
		c.collectionsPartialView.Mutex.Lock()
		defer c.collectionsPartialView.Mutex.Unlock()

		_, exists := c.collectionsPartialView.KnownCollections[collectionId]
		if exists {
			logger.Println("HTTP-REQ", "Collection already exists", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)

		// Process the new collection creation
		if isStrong {
			c.collectionsPartialView.KnownCollections[collectionId] = shared.NewStrongCollection(collectionId, 0, true, false) // remote strong collection
		} else {
			c.collectionsPartialView.KnownCollections[collectionId] = shared.NewWeakCollection(collectionId, 0, true) // remote weak collection
		}
		c.collectionsPartialView.KnownCollections[collectionId].AddHost(msg.SenderIp)

		//propagate update to father
		if c.serverInfo.Father != nil {
			msg.SenderIp = c.serverInfo.HostName
			c.sender.NewCollectionCreated(c.serverInfo.Father.HostName, msg)
		}

	})
}

func (c *CollectionsDiscovererImp) handleIncomingRemoveCollection(logger log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Println("HTTP-REQ", "New collection removal request received through HTTP request", log.INFO)

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Println("HTTP-REQ", "Error reading request body", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var msg gossip.RemoveCollectionMessage
		if err := json.Unmarshal(bodyBytes, &msg); err != nil {
			logger.Println("HTTP-REQ", "Error parsing new collection message", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		collectionId := msg.CollectionId
		senderIp := msg.SenderIp
		w.WriteHeader(http.StatusOK)

		//async
		dbErr := c.RemoveCollectionAndProgate(collectionId, migrationManager.GetMigrationManagerInstance(c.logger), false, senderIp)
		if dbErr != nil {
			logger.Println("HTTP-REQ", "Error removing collection: "+dbErr.Error(), log.ERROR)
			return
		}
	})
}
