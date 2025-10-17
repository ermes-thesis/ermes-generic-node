package strongPolicy

import (
	"sync"
	"sync/atomic"
)

type MyMap struct {
	m             sync.Map
	resourceCount int32
}

func (m *MyMap) Load(key string) (any, bool) {
	return m.m.Load(key)
}

func (m *MyMap) Store(key string, value any) {
	m.m.Store(key, value)
	atomic.AddInt32(&m.resourceCount, 1)
}

func (m *MyMap) Delete(key string) {
	m.m.Delete(key)
	atomic.AddInt32(&m.resourceCount, -1)
}

func (m *MyMap) Range(f func(key, value any) bool) {
	m.m.Range(f)
}

func (m *MyMap) Count() int {
	return int(atomic.LoadInt32(&m.resourceCount))
}
