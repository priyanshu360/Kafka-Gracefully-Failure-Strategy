package counter

import "sync"

type MessageCounter struct {
	mu    sync.Mutex
	count int
}

func (mc *MessageCounter) Increment() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.count++
}

func (mc *MessageCounter) GetMessageCount() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.count
}
