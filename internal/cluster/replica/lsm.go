package replica

import "sync"

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
	}
}

func (l *LSM) AddLogEntry(logEntry ReplicaLog) {
	l.mu.Lock()
	l.mem.data = append(l.mem.data, logEntry)
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
