package collectionsDiscoverer

import (
	gossip "dataManager/messages"
	"dataManager/migrationManager"
	shared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	routing "github.com/qiangxue/fasthttp-routing"
)

func (c *CollectionsDiscovererImp) handleIncomingRemoteAccess(ctx *routing.Context) error {

	bodyMsg := ctx.PostBody()

	c.log.Eval("Received remote access request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	var msg gossip.RemoteAccessRequest
	if err := json.Unmarshal(bodyMsg, &msg); err != nil {
		c.log.Error("Failed to decode request payload", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", err)
		ctx.Error("Bad Request: Failed to parse request body", http.StatusBadRequest)

		return errors.New("bad Request: Failed to parse request body")
	}
	c.log.Debug("Request body read successfully", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	var res any
	var dbErr error
	var resp gossip.RemoteAccessResponse

	if len(msg.RecordNames) == 0 {
		c.log.Warn("Remote access request contains no record names", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg))
		ctx.Error("Bad Request: No record names specified in the body message", http.StatusBadRequest)
		return errors.New("bad Request: No record names specified in the body message")
	}

	if len(msg.RecordNames) == 1 {
		c.log.Debug("Single-record remote access", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg))
		dbFn := func() (any, error) {
			c.log.Debug("Accessing the record value locally through dbFn", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups)
			authorized, err := c.dbInterface.IsAuthorized(msg.CollectionId, msg.UserId, msg.Groups...)
			if err != nil {
				c.log.Error("Failed to check user authorization access", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups, "error", err)
				return nil, err
			}
			if !authorized {
				c.log.Error("User is not authorized to retrieve collection record", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups)
				return nil, &shared.UnauthorizedError{}
			}

			value, err := c.dbInterface.GetRecord(msg.CollectionId, msg.RecordNames[0])
			if err != nil {
				c.log.Error("Failed to retrieve collection's record value", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups, "error", err)
				return nil, err
			}
			return value, nil
		}
		remoteFn := func(addr string) (any, error) {
			c.log.Debug("Accessing the record value remotely through remoteFn", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups, "towards_to", addr)
			return c.WeakCollectionRemoteRecordAccess(addr, msg.CollectionId, msg.Groups, msg.UserId, msg.RecordNames[0])
		}
		res, dbErr = c.AccessRecords(msg.CollectionId, dbFn, remoteFn)
		if dbErr != nil {
			var unAuthErr *shared.UnauthorizedError
			if errors.As(dbErr, &unAuthErr) {
				c.log.Error("User is unauthorized to access the requested record", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", unAuthErr)
				ctx.Error("Unauthorized: User is not authorized to access the specified record", http.StatusUnauthorized)
				return errors.New("unauthorized: User is not authorized to access the specified record")
			}

			c.log.Error("Failed to access the specified record", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", dbErr)
			ctx.Error("Internal Server Error: Failed to access the specified record", http.StatusInternalServerError)
			return errors.New("internal Server Error: Failed to access the specified record")
		}

		resp = gossip.RemoteAccessResponse{
			Records: []any{res},
		}
	} else {
		c.log.Debug("Multi-record remote access", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg))
		dbFn := func() (any, error) {
			c.log.Debug("Accessing the record value locally through dbFn", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups)
			authorized, err := c.dbInterface.IsAuthorized(msg.CollectionId, msg.UserId, msg.Groups...)
			if err != nil {
				c.log.Error("Failed to check user authorization access", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups, "error", err)
				return nil, err
			}
			if !authorized {
				c.log.Error("User is not authorized to retrieve collection records", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups)
				return nil, &shared.UnauthorizedError{}
			}

			values, err := c.dbInterface.GetRecords(msg.CollectionId, msg.RecordNames...)
			if err != nil {
				c.log.Error("Failed to retrieve collection's records values", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups, "error", err)
				return nil, err
			}
			return values, nil
		}
		remoteFn := func(addr string) (interface{}, error) {
			c.log.Debug("Accessing the records values remotely through remoteFn", "collection_id", msg.CollectionId, "executing_user", msg.UserId, "user_access_groups", msg.Groups, "towards_to", addr)
			return c.WeakCollectionRemoteRecordsAccess(addr, msg.CollectionId, msg.Groups, msg.UserId, msg.RecordNames...)
		}
		res, dbErr = c.AccessRecords(msg.CollectionId, dbFn, remoteFn)
		if dbErr != nil {
			var unAuthErr *shared.UnauthorizedError
			if errors.As(dbErr, &unAuthErr) {
				c.log.Error("User is unauthorized to access the requested records", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", unAuthErr)
				ctx.Error("Unauthorized: User is not authorized to access the specified records", http.StatusUnauthorized)
				return errors.New("unauthorized: User is not authorized to access the specified records")
			}

			c.log.Error("Failed to access the specified records", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", dbErr)
			ctx.Error("Internal Server Error: Failed to access the specified records", http.StatusInternalServerError)
			return errors.New("internal Server Error: Failed to access the specified records")
		}

		resp = gossip.RemoteAccessResponse{
			Records: res.([]any),
		}
	}

	ctx.SetContentType("application/json")
	encoded, err := json.Marshal(resp)
	if err != nil {
		c.log.Error("Failed to convert response into json", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "response", fmt.Sprintf("%+v", res), "error", err)
		ctx.Error(fmt.Sprintf("Internal Server Error: Failed to convert response into json: %v", err), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Internal Server Error: Failed to convert response into json: %v", err))
	}

	_, err = ctx.Write(encoded)
	if err != nil {
		c.log.Error("Failed to encode response into body", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "response", fmt.Sprintf("%+v", res), "error", err)
		ctx.Error(fmt.Sprintf("Internal Server Error: Failed to encode response into body: %v", err), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Internal Server Error: Failed to encode response into body: %v", err))
	}

	c.log.Info("Remote access request completed successfully", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())
	c.log.Debug("Remote access request completed successfully", "response", fmt.Sprintf("%+v", res))
	return nil
}

func (c *CollectionsDiscovererImp) handleIncomingUpdatePropagation(ctx *routing.Context) error {

	bodyMsg := ctx.PostBody()

	c.log.Eval("Received update propagation request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	var msg gossip.CollectionUpdateMessages
	if err := json.Unmarshal(bodyMsg, &msg); err != nil {
		c.log.Error("Failed to decode request payload", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", err)
		ctx.Error("Bad Request: Failed to parse request body", http.StatusBadRequest)
		return errors.New("bad Request: Failed to parse request body")
	}
	c.log.Debug("Request body read successfully", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	c.log.Info("Propagating collections updates", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg))
	err := c.UpdateCollectionsRecordsAndPropagate(msg.Messages, msg.Groups, msg.UserId, false, msg.Messages[0].SenderIp)
	if err != nil {
		c.log.Error("Failed to propagate collections updates", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", err)
		ctx.Error("Internal Server Error: Failed to propagate collections updates", http.StatusInternalServerError)
		return errors.New("internal Server Error: Failed to propagate collections updates")
	}
	ctx.SetStatusCode(http.StatusOK) //response
	return nil
}

func (c *CollectionsDiscovererImp) handleIncomingUpdateGroupPropagation(ctx *routing.Context) error {

	bodyMsg := ctx.PostBody()

	c.log.Eval("Received group update propagation request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	var msg gossip.CollectionGroupUpdateMessages
	if err := json.Unmarshal(bodyMsg, &msg); err != nil {
		c.log.Error("Failed to decode request payload", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", err)
		ctx.Error("Bad Request: Failed to parse request body", http.StatusBadRequest)
		return errors.New("bad Request: Failed to parse request body")
	}
	c.log.Debug("Request body read successfully", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	c.log.Info("Propagating collection groups updates", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg))
	err := c.UpdateCollectionsGroupsAndPropagate(msg, false, msg[0].SenderIp)
	if err != nil {
		c.log.Error("Failed to propagate collection groups updates", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", err)
		ctx.Error("Internal Server Error: Failed to propagate collection groups updates", http.StatusInternalServerError)
		return errors.New("internal Server Error: Failed to propagate collection groups updates")
	}
	ctx.SetStatusCode(http.StatusOK) //response
	return nil
}

func (c *CollectionsDiscovererImp) handleIncomingNewCollection(ctx *routing.Context) error {

	bodyMsg := ctx.PostBody()

	c.log.Eval("Received new collection creation request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	var msg gossip.NewCollectionMessage
	if err := json.Unmarshal(bodyMsg, &msg); err != nil {
		c.log.Error("Failed to decode request payload", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", err)
		ctx.Error("Bad Request: Failed to parse request body", http.StatusBadRequest)
		return errors.New("bad Request: Failed to parse request body")
	}
	c.log.Debug("Request body read successfully", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	collectionId := msg.CollectionId
	isStrong := msg.IsStrong
	senderIp := msg.SenderIp
	leaderIp := msg.LeaderIp

	//sync
	c.collectionsPartialView.Mutex.Lock()
	defer c.collectionsPartialView.Mutex.Unlock()

	c.log.Info("Processing new collection creation", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg))

	_, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if exists {
		c.log.Error("Duplicate collection ID detected. A collection with the same UUID already exists", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "message", fmt.Sprintf("%+v", msg))
		ctx.Error("Duplicate collection ID detected", http.StatusConflict)
		return errors.New("duplicate collection ID detected")
	}

	//propagate update to father
	if c.serverInfo.Father != nil {
		c.log.Info("Propagating new collection creation to father node", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg), "father_hostname", c.serverInfo.Father.HostName)
		msg.SenderIp = c.serverInfo.HostName
		msg.LeaderIp = c.serverInfo.HostName
		err := c.sender.NewCollectionCreated(c.serverInfo.Father.HostName, msg)
		if err != nil {
			c.log.Error("Failed to propagate new collection creation to father node", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg), "error", err)
			ctx.Error(fmt.Sprintf("Error creating new collection: %v", err), http.StatusInternalServerError)
			return errors.New(fmt.Sprintf("Error creating new collection: %v", err))
		}
	} else {
		if !msg.SkipUniquenessCheck {
			ok, err := c.dbInterface.IsUniqueForOwner(msg.CollectionName, msg.CollectionOwner)
			if err != nil {
				c.log.Error("Error checking uniqueness for new collection", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg), "error", err)
				ctx.Error(fmt.Sprintf("Error checking if collection is unique: %v", err), http.StatusInternalServerError)
				return errors.New(fmt.Sprintf("Error checking if collection is unique: %v", err))
			}
			if !ok {
				c.log.Warn("Duplicate collection name detected for same owner", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_name", msg.CollectionName, "owner", msg.CollectionOwner)
				ctx.Error("Cannot create collection, since another one with the same name and owner already exists", http.StatusConflict)
				return errors.New("cannot create collection, since another one with the same name and owner already exists")
			}
		}
	}

	// Process the new collection creation
	if isStrong {
		c.collectionsPartialView.KnownCollections[collectionId] = shared.NewStrongCollection(collectionId, 0, true, false, leaderIp) // remote strong collection
	} else {
		c.collectionsPartialView.KnownCollections[collectionId] = shared.NewWeakCollection(collectionId, 0, true) // remote weak collection
	}

	c.log.Debug("Adding message sender hostname to the Hosts directions for the collection instance in the current node", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "node_hostname", c.serverInfo.HostName, "added_hostname(sender)", senderIp, "current_known_hosts", c.collectionsPartialView.KnownCollections[collectionId].HostsId())
	c.collectionsPartialView.KnownCollections[collectionId].AddHost(senderIp)
	c.log.Debug("Host added to collection successfully", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "updated_known_hosts", c.collectionsPartialView.KnownCollections[collectionId].HostsId())

	c.log.Info("New collection creation completed successfully", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())
	ctx.SetStatusCode(http.StatusOK)
	return nil
}

func (c *CollectionsDiscovererImp) handleIncomingRemoveCollection(ctx *routing.Context) error {

	bodyMsg := ctx.PostBody()

	c.log.Eval("Received collection removal request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	var msg gossip.RemoveCollectionMessage
	if err := json.Unmarshal(bodyMsg, &msg); err != nil {
		c.log.Error("Failed to decode request payload", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", err)
		ctx.Error("Bad Request: Failed to parse request body", http.StatusBadRequest)
		return errors.New("bad Request: Failed to parse request body")
	}
	c.log.Debug("Request body read successfully", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

	collectionId := msg.CollectionId
	senderIp := msg.SenderIp

	//async
	dbErr := c.RemoveCollectionAndPropagate(collectionId, migrationManager.GetMigrationManagerInstance(), msg.Groups, msg.UserId, false, senderIp)
	if dbErr != nil {
		c.log.Error("Failed collection removal propagation", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg), "error", dbErr)
		ctx.Error("Internal Server Error: Failed to propagate collection removal", http.StatusInternalServerError)
		return errors.New("internal Server Error: Failed to propagate collection removal")
	}

	c.log.Info("Collection removed successfully", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId)
	ctx.SetStatusCode(http.StatusOK)
	return nil
}

func (c *CollectionsDiscovererImp) handleIncomingRemoteCollectionSet(migrationManager migrationManager.MigrationManager) routing.Handler {
	return func(ctx *routing.Context) error {

		bodyMsg := ctx.PostBody()

		c.log.Eval("Received remote collection set request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())

		var msg gossip.SetCollectionMessage
		if err := json.Unmarshal(bodyMsg, &msg); err != nil {
			c.log.Error("Failed to decode request payload", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "error", err)
			ctx.Error("Bad Request: Failed to parse request body", http.StatusBadRequest)
			return errors.New("bad Request: Failed to parse request body")
		}

		//collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, migrationManager migrationManager.MigrationManager, isStrong, skipUniquenessCheck bool
		err := c.SetCollectionAndPropagate(msg.CollectionId, msg.CollectionName, msg.Records, msg.Groups, msg.UserId, migrationManager, msg.IsStrong, msg.SkipUniquenessCheck)

		if err != nil {
			c.log.Error("Failed collection set", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "message", fmt.Sprintf("%+v", msg), "error", err)
			ctx.Error("Internal Server Error: Failed collection set", http.StatusInternalServerError)
			return errors.New("internal Server Error: Failed collection set")
		}

		ctx.SetStatusCode(http.StatusOK)
		return nil
	}
}
