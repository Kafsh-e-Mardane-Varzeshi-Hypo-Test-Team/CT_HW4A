package replica

import (
	"sort"
	"sync"
)

const maxLen int = 1e4

type MemTable struct {
	data []ReplicaLog
}

type Snapshot struct {
	Tables []MemTable
}

type LSM struct {
	mu         sync.RWMutex
	mem        *MemTable
	immutables []*MemTable
	wal        []ReplicaLog
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: []ReplicaLog{},
	}
}

func NewLSM() *LSM {
	return &LSM{
		mu:         sync.RWMutex{},
		mem:        NewMemTable(),
		immutables: []*MemTable{},
		wal:        []ReplicaLog{},
	}
}

func (l *LSM) AddLogEntry(logEntry ReplicaLog) {
	l.mu.Lock()
	l.mem.data = append(l.mem.data, logEntry)
	l.wal = append(l.wal, logEntry)
	l.mu.Unlock()

	if len(l.mem.data) >= maxLen {
		l.flush()
	}
}

func (l *LSM) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.mem.data) == 0 {
		return
	}

	l.immutables = append(l.immutables, l.mem)
	l.mem = NewMemTable()
}

func (l *LSM) WALAfter(ts int64) []ReplicaLog {
	l.mu.RLock()
	defer l.mu.RUnlock()

	idx := sort.Search(len(l.wal), func(i int) bool {
		return !(l.wal[i].Timestamp < ts)
	})
	return l.wal[idx:] // return a copy
}
