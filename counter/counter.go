package counter

import "sync"

type MessageCounter struct {
	mu    sync.Mutex
	count int
	set   map[string]bool
}

func NewMessageCounter() MessageCounter {
	return MessageCounter{
		mu:    sync.Mutex{},
		count: 0,
		set:   make(map[string]bool),
	}
}

func (mc *MessageCounter) Increment(message string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.count++
	mc.set[message] = true
}

func (mc *MessageCounter) GetMessageCount() (int, *map[string]bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.count, &mc.set
}
