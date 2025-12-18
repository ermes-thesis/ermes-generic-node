package migrationManager

import (
	gossip "dataManager/messages"
	"encoding/json"
	"errors"
	"net/http"

	routing "github.com/qiangxue/fasthttp-routing"
)

func (m *MigrationManagerImp) handleIncomingGossipMessages(c *routing.Context) error {

	m.log.Eval("HTTP-REQ, New gossip message received through HTTP request", "remote_addr", c.RemoteAddr())

	// No more deferred functions

	bodyBytes := c.PostBody()
	if bodyBytes == nil {
		m.log.Error("HTTP-REQ, Error reading request body")
		c.SetStatusCode(http.StatusBadRequest)
		return errors.New("error reading request body")
	}

	var baseMsg gossip.BaseGossipMessage
	if err := json.Unmarshal(bodyBytes, &baseMsg); err != nil {
		m.log.Error("HTTP-REQ, Error parsing base message")
		c.SetStatusCode(http.StatusBadRequest)
		return errors.New("error parsing base message")
	}

	var concreteMsg gossip.GossipMessage

	switch baseMsg.MessageType {
	case "HW_STATE":
		var msg gossip.ChildStateMessage
		if err := msg.FromJSON(bodyBytes); err != nil {
			m.log.Error("HTTP-REQ, Error parsing HW_STATE message")
			c.SetStatusCode(http.StatusBadRequest)
			return errors.New("error parsing HW_STATE message")
		}
		concreteMsg = &msg

	case "LEADER_DELEGA":
		var msg gossip.LeaderDelegaMessage
		if err := msg.FromJSON(bodyBytes); err != nil {
			m.log.Error("HTTP-REQ, Error parsing LEADER_DELEGA message")
			c.SetStatusCode(http.StatusBadRequest)
			return errors.New("error parsing LEADER_DELEGA message")
		}
		concreteMsg = &msg

	default:
		m.log.Error("HTTP-REQ, Unknown message type: " + baseMsg.MessageType)
		c.SetStatusCode(http.StatusBadRequest)
		return errors.New("unknown message type")
	}

	m.ManageGossipMessage(concreteMsg)

	c.SetStatusCode(http.StatusOK)
	return nil
}

func (m *MigrationManagerImp) handleIncomingCollection(c *routing.Context) error {
	m.log.Eval("HTTP-REQ, New collection received through HTTP request", "remote_addr", c.RemoteAddr())

	bodyBytes := c.PostBody()
	if bodyBytes == nil {
		m.log.Error("HTTP-REQ, Error reading request body")
		c.SetStatusCode(http.StatusBadRequest)
		return errors.New("error reading request body")
	}

	var collection gossip.SendCollectionMessage
	if err := json.Unmarshal(bodyBytes, &collection); err != nil {
		m.log.Error("HTTP-REQ, Error parsing collection message")
		c.SetStatusCode(http.StatusBadRequest)
		return errors.New("error parsing collection message")
	}

	m.log.Info("HTTP-REQ, New collection received info", "collection_id", collection.CollectionId, "is_strong", collection.IsStrong, "is_leader", collection.IsLeader, "records_count", len(collection.Records), "groups", collection.Groups, "owner", collection.Owner, "groups_sequence", collection.GroupsSequence, "collection_size", collection.CollectionSize, "host_to_remove", collection.HostToRemove, "sender_ip", collection.SenderIp)

	m.NewCollectionReceived(collection.CollectionId, collection.IsStrong, collection.SequenceNumber, collection.IsLeader, collection.Records, collection.Groups, collection.Owner, collection.GroupsSequence, collection.CollectionSize, collection.HostToRemove, collection.SenderIp)

	c.SetStatusCode(http.StatusOK)
	return nil
}

func (m *MigrationManagerImp) handleIncomingRequestInfoMessages(c *routing.Context) error {
	m.log.Eval("HTTP-REQ, New collection request info message received through HTTP request", "remote_addr", c.RemoteAddr())

	bodyBytes := c.PostBody()
	if bodyBytes == nil {
		m.log.Error("HTTP-REQ, Error reading request body")
		c.SetStatusCode(http.StatusBadRequest)
		return errors.New("error reading request body")
	}

	var msg gossip.CollectionRequestInfoMessages
	if err := json.Unmarshal(bodyBytes, &msg); err != nil {
		m.log.Error("HTTP-REQ, Error parsing collection request info message")
		c.SetStatusCode(http.StatusBadRequest)
		return errors.New("error parsing collection request info message")
	}

	c.SetStatusCode(http.StatusOK) //response

	m.updatesRequestsInfo(msg)
	return nil
}
