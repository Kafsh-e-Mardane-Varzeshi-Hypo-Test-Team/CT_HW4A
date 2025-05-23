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

// sortMemTableByKey sorts the entries of a MemTable by key (Data field in this example).
func sortMemTableByKey(mt *MemTable) {
	sort.Slice(mt.data, func(i, j int) bool {
		return mt.data[i].Key < mt.data[j].Key
	})
}

// mergeOldest merges two sorted MemTables, keeping only the oldest ReplicaLog for each key.
func mergeOldest(a, b *MemTable) *MemTable {
	result := &MemTable{data: make([]ReplicaLog, 0, len(a.data)+len(b.data))}
	i, j := 0, 0

	for i < len(a.data) && j < len(b.data) {
		if a.data[i].Key < b.data[j].Key {
			result.data = append(result.data, a.data[i])
			i++
		} else if a.data[i].Key > b.data[j].Key {
			result.data = append(result.data, b.data[j])
			j++
		} else {
			// Same key, pick the older one
			if a.data[i].Timestamp < (b.data[j].Timestamp) {
				result.data = append(result.data, a.data[i])
			} else {
				result.data = append(result.data, b.data[j])
			}
			i++
			j++
		}
	}

	for i < len(a.data) {
		result.data = append(result.data, a.data[i])
		i++
	}
	for j < len(b.data) {
		result.data = append(result.data, b.data[j])
		j++
	}

	return result
}

// CompactImmutables merges the first two immutable MemTables, deduplicating by key with oldest entry.
func (l *LSM) CompactImmutables() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.immutables) < 2 {
		return // Nothing to compact
	}

	// Sort first two memtables
	sortMemTableByKey(l.immutables[0])
	sortMemTableByKey(l.immutables[1])

	// Merge them
	merged := mergeOldest(l.immutables[0], l.immutables[1])

	// Replace first two with the merged table
	l.immutables = append([]*MemTable{merged}, l.immutables[2:]...)
}
