package migrationManager

import (
	gossip "dataManager/messages"
	"encoding/json"
	"io"
	"net"
	"net/http"
)

func (m *MigrationManagerImp) handleIncomingGossipMessages() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.log.Info("HTTP-REQ, New gossip message received through HTTP request")

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			m.log.Error("HTTP-REQ, Error reading request body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var baseMsg gossip.BaseGossipMessage
		if err := json.Unmarshal(bodyBytes, &baseMsg); err != nil {
			m.log.Error("HTTP-REQ, Error parsing base message")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var concreteMsg gossip.GossipMessage

		switch baseMsg.MessageType {
		case "HW_STATE":
			var msg gossip.ChildStateMessage
			if err := msg.FromJSON(bodyBytes); err != nil {
				m.log.Error("HTTP-REQ, Error parsing HW_STATE message")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			concreteMsg = &msg

		case "LEADER_DELEGA":
			var msg gossip.LeaderDelegaMessage
			if err := msg.FromJSON(bodyBytes); err != nil {
				m.log.Error("HTTP-REQ, Error parsing LEADER_DELEGA message")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			concreteMsg = &msg

		default:
			m.log.Error("HTTP-REQ, Unknown message type: " + baseMsg.MessageType)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.ManageGossipMessage(concreteMsg)

		w.WriteHeader(http.StatusOK)
	})
}

func (m *MigrationManagerImp) handleIncomingCollection() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.log.Info("HTTP-REQ, New collection received through HTTP request")

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			m.log.Error("HTTP-REQ, Error reading request body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var collection gossip.SendCollectionMessage
		if err := json.Unmarshal(bodyBytes, &collection); err != nil {
			m.log.Error("HTTP-REQ, Error parsing collection message")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		ip, port, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		address := ip + ":" + port //TODO: verify

		m.NewCollectionReceived(collection.CollectionId, collection.IsStrong, collection.IsLeader, collection.Records, collection.Groups, collection.GroupsSequence, collection.CollectionSize, collection.HostToRemove, address)

		w.WriteHeader(http.StatusOK)
	})
}
