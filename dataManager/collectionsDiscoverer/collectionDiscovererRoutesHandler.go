package collectionsDiscoverer

import (
	gossip "dataManager/messages"
	"dataManager/migrationManager"
	shared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func (c *CollectionsDiscovererImp) handleIncomingRemoteAccess() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.log.Info("HTTP-REQ, New remote access request received through HTTP request")

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			c.log.Error("HTTP-REQ, Error reading request body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var msg gossip.RemoteAccessRequest
		if err := json.Unmarshal(bodyBytes, &msg); err != nil {
			c.log.Error("HTTP-REQ, Error parsing remote access request")
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
				authorized, err := c.dbInterface.IsAuthorized(msg.CollectionId, msg.UserId, msg.Groups...)
				if err != nil {
					return nil, err
				}
				if !authorized {
					return nil, &shared.UnauthorizedError{}
				}

				return c.dbInterface.GetRecord(msg.CollectionId, msg.RecordNames[0])
			}
			remoteFn := func(addr string) (interface{}, error) {
				return c.WeakCollectionRemoteRecordAccess(addr, msg.CollectionId, msg.Groups, msg.UserId, msg.RecordNames[0])
			}
			res, dbErr = AccessRecords(c, msg.CollectionId, dbFn, remoteFn)
			if dbErr != nil {
				var unAuthErr *shared.UnauthorizedError
				if errors.As(dbErr, &unAuthErr) {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		} else {
			dbFn := func() (interface{}, error) {
				authorized, err := c.dbInterface.IsAuthorized(msg.CollectionId, msg.UserId, msg.Groups...)
				if err != nil {
					return nil, err
				}
				if !authorized {
					return nil, &shared.UnauthorizedError{}
				}

				return c.dbInterface.GetRecords(msg.CollectionId, msg.RecordNames...)
			}
			remoteFn := func(addr string) (interface{}, error) {
				return c.WeakCollectionRemoteRecordsAccess(addr, msg.CollectionId, msg.Groups, msg.UserId, msg.RecordNames...)
			}
			res, dbErr = AccessRecords(c, msg.CollectionId, dbFn, remoteFn)
			if dbErr != nil {
				var unAuthErr *shared.UnauthorizedError
				if errors.As(dbErr, &unAuthErr) {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

		}

		resp := gossip.RemoteAccessResponse{
			Records: res.([]any),
		}

		jsonResp, jsonErr := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")

		if dbErr != nil || jsonErr != nil {
			c.log.Error("HTTP-REQ, Error accessing collection: " + dbErr.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Write(jsonResp)

		w.WriteHeader(http.StatusOK)
	})
}

func (c *CollectionsDiscovererImp) handleIncomingUpdatePropagation() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.log.Info("HTTP-REQ, New update propagation request received through HTTP request")

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			c.log.Error("HTTP-REQ, Error reading request body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var msg gossip.CollectionUpdateMessages
		if err := json.Unmarshal(bodyBytes, &msg); err != nil {
			c.log.Error("HTTP-REQ: Error parsing remote propagation request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK) //response

		c.UpdateCollectionsRecordsAndPropagate(msg.Messages, msg.Groups, msg.UserId, false, msg.Messages[0].SenderIp)
	})
}

func (c *CollectionsDiscovererImp) handleIncomingUpdateGroupPropagation() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.log.Info("HTTP-REQ", "New group update propagation request received through HTTP request")

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			c.log.Error("HTTP-REQ, Error reading request body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var msg gossip.CollectionGroupUpdateMessages
		if err := json.Unmarshal(bodyBytes, &msg); err != nil {
			c.log.Error("HTTP-REQ", "Error parsing remote group propagation request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK) //response

		c.UpdateCollectionsGroupsAndPropagate(msg, false, msg[0].SenderIp)
	})
}

func (c *CollectionsDiscovererImp) handleIncomingNewCollection() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.log.Info("HTTP-REQ: New collection creation request received through HTTP request")

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			c.log.Error("HTTP-REQ: Error reading request body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var msg gossip.NewCollectionMessage
		if err := json.Unmarshal(bodyBytes, &msg); err != nil {
			c.log.Error("HTTP-REQ: Error parsing new collection message")
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
			c.log.Error("HTTP-REQ: Collection already exists")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		//propagate update to father
		if c.serverInfo.Father != nil {
			msg.SenderIp = c.serverInfo.HostName
			err = c.sender.NewCollectionCreated(c.serverInfo.Father.HostName, msg)
			if err != nil {
				http.Error(w, fmt.Sprintf("Error creating new collection: %v", err), http.StatusInternalServerError)
				return
			}
		} else {
			ok, err := c.dbInterface.IsUniqueForOwner(msg.CollectionName, msg.CollectionOwner)
			if err != nil {
				c.log.Error("HTTP-REQ: Error checking if collection is unique")
				http.Error(w, fmt.Sprintf("Error checking if collection is unique: %v", err), http.StatusInternalServerError)
				return
			}
			if !ok {
				c.log.Error("HTTP-REQ: Error checking if collection is unique")
				http.Error(w, fmt.Sprintf("Cannot create collection, since another one with the same name already exists: %v", err), http.StatusConflict)
				return
			}
		}

		// Process the new collection creation
		if isStrong {
			c.collectionsPartialView.KnownCollections[collectionId] = shared.NewStrongCollection(collectionId, 0, true, false) // remote strong collection
		} else {
			c.collectionsPartialView.KnownCollections[collectionId] = shared.NewWeakCollection(collectionId, 0, true) // remote weak collection
		}
		c.collectionsPartialView.KnownCollections[collectionId].AddHost(msg.SenderIp)

		w.WriteHeader(http.StatusOK)
	})
}

func (c *CollectionsDiscovererImp) handleIncomingRemoveCollection() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.log.Info("HTTP-REQ: New collection removal request received through HTTP request")

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			c.log.Error("HTTP-REQ: Error reading request body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var msg gossip.RemoveCollectionMessage
		if err := json.Unmarshal(bodyBytes, &msg); err != nil {
			c.log.Error("HTTP-REQ", "Error parsing new collection message")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		collectionId := msg.CollectionId
		senderIp := msg.SenderIp
		w.WriteHeader(http.StatusOK)

		//async
		dbErr := c.RemoveCollectionAndPropagate(collectionId, migrationManager.GetMigrationManagerInstance(), msg.Groups, msg.UserId, false, senderIp)
		if dbErr != nil {
			c.log.Error("HTTP-REQ", "Error removing collection: "+dbErr.Error())
		}
	})
}
