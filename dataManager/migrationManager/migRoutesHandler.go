package migrationManager

import (
	gossip "dataManager/messages"
	"encoding/json"
	"io"
	log "logger"
	"net/http"
)

func (m *MigrationManagerImp) handleIncomingGossipMessages(logger log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Println("HTTP-REQ", "New gossip message received through HTTP request", log.INFO)

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Println("HTTP-REQ", "Error reading request body", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var baseMsg gossip.BaseGossipMessage
		if err := json.Unmarshal(bodyBytes, &baseMsg); err != nil {
			logger.Println("HTTP-REQ", "Error parsing base message", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var concreteMsg gossip.GossipMessage

		switch baseMsg.MessageType {
		case "HW_STATE":
			var msg gossip.ChildStateMessage
			if err := msg.FromJSON(bodyBytes); err != nil {
				logger.Println("HTTP-REQ", "Error parsing HW_STATE message", log.ERROR)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			concreteMsg = &msg

		case "LEADER_DELEGA":
			var msg gossip.LeaderDelegaMessage
			if err := msg.FromJSON(bodyBytes); err != nil {
				logger.Println("HTTP-REQ", "Error parsing LEADER_DELEGA message", log.ERROR)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			concreteMsg = &msg

		default:
			logger.Println("HTTP-REQ", "Unknown message type: "+baseMsg.MessageType, log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.ManageGossipMessage(concreteMsg)

		w.WriteHeader(http.StatusOK)
	})
}

func (m *MigrationManagerImp) handleIncomingCollection(logger log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Println("HTTP-REQ", "New collection received through HTTP request", log.INFO)

		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Println("HTTP-REQ", "Error reading request body", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var collection gossip.SendCollectionMessage
		if err := json.Unmarshal(bodyBytes, &collection); err != nil {
			logger.Println("HTTP-REQ", "Error parsing collection message", log.ERROR)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		//TODO: capire come stimare dimensioni e come capire se strong o weak
		m.NewCollectionReceived(collection.CollectionId, collection.IsStrong, collection.IsLeader, collection.Records, collection.Groups, collection.CollectionSize)

		w.WriteHeader(http.StatusOK)
	})
}
