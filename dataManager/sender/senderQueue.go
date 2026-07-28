package sender

import (
	shared "dataManager/shared"
	"sync"
)

type outboundMessage struct {
	hostId      string
	path        string
	payload     any
	saftyQueues []shared.FunctionQueue
}

type HostQueue struct {
	channel chan outboundMessage
}

type MultiQueue struct {
	queueMu sync.Mutex
	queues  map[string]*HostQueue // hostID -> queue
	consume func(outboundMessage)
}

func NewMultiQueue(consume func(outboundMessage)) *MultiQueue {
	return &MultiQueue{
		queues:  make(map[string]*HostQueue),
		consume: consume,
	}
}

func (mq *MultiQueue) getOrCreateQueue(hostId string) *HostQueue {
	mq.queueMu.Lock()
	defer mq.queueMu.Unlock()

	val, ok := mq.queues[hostId]
	if !ok {
		hq := &HostQueue{channel: make(chan outboundMessage, 128)}
		mq.queues[hostId] = hq
		go func() {
			for msg := range hq.channel {
				mq.consume(msg)
			}
		}()
		return hq
	}
	return val
}
