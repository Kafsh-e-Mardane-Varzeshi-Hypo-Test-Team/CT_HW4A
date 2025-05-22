package replica

import (
	"log"
	"strconv"
	"sync"
)

type ReplicaType int

const (
	Follower ReplicaType = iota
	Leader
)

type Replica struct {
	Id          int
	NodeId      int
	PartitionId int
	Mode        ReplicaType

	data      map[string]ReplicaData
	lsm       *LSM
	timestamp int64 // only used in Leader type
	mu        sync.RWMutex
}

func NewReplica(id, nodeId, partitionId int, mode ReplicaType) *Replica {
	return &Replica{
		Id:          id,
		NodeId:      nodeId,
		PartitionId: partitionId,
		Mode:        mode,
		data:        make(map[string]ReplicaData),
		lsm:         NewLSM(),
		timestamp:   0,
		mu:          sync.RWMutex{},
	}
}

// timestamp doesn't matter for leader, pass -1.
func (r *Replica) Set(key, value string, timestamp int64) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var ts int64
	if r.Mode == Leader {
		ts = r.timestamp
		r.timestamp++
	} else {
		ts = timestamp
		// Reject if data is newer
		// TODO: check if timestamp collision can happen
		if currentData, exists := r.data[key]; exists && currentData.Timestamp >= ts {
			err := &OldSetRequestError{
				key:              key,
				oldTimestamp:     currentData.Timestamp,
				oldValue:         currentData.Value,
				requestTimestamp: ts,
				newValue:         value,
			}
			log.Println(err.Error())
			return ReplicaLog{}, err
		}
	}

	r.data[key] = ReplicaData{Value: value, Timestamp: ts}

	logEntry := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionSet,
		Timestamp:   ts,
		Key:         key,
		Value:       value,
	}

	if r.Mode == Leader {
		r.lsm.AddLogEntry(logEntry)
	}

	log.Println(r.modePrefix() + "SetKey: " + logEntry.String())
	return logEntry, nil
}

func (r *Replica) Get(key string) (ReplicaLog, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data, ok := r.data[key]
	if !ok {
		err := &KeyNotFoundError{key}
		log.Println(err.Error())
		return ReplicaLog{}, err
	}

	logEntry := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionGet,
		Timestamp:   -1,
		Key:         key,
		Value:       data.Value,
	}
	log.Println(r.modePrefix() + "GetKey: " + logEntry.String())
	return logEntry, nil
}

// timestamp doesn't matter for leader, pass -1.
func (r *Replica) Delete(key string, timestamp int64) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	data, ok := r.data[key]
	if !ok {
		err := &KeyNotFoundError{key}
		log.Println(err.Error())
		return ReplicaLog{}, err
	}

	var ts int64
	if r.Mode == Leader {
		ts = r.timestamp
		r.timestamp++
	} else {
		ts = timestamp
		// TODO: check if timestamp collision can happen
		if data.Timestamp >= ts {
			err := &OldDeleteRequestError{
				key:              key,
				oldValue:         data.Value,
				oldTimestamp:     data.Timestamp,
				requestTimestamp: timestamp,
			}
			log.Println(err.Error())
			return ReplicaLog{}, err
		}
	}

	delete(r.data, key)

	logEntry := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionDelete,
		Timestamp:   ts,
		Key:         key,
		Value:       "",
	}

	if r.Mode == Leader {
		r.lsm.AddLogEntry(logEntry)
	}

	log.Println(r.modePrefix() + "DeleteKey: " + logEntry.String())
	return logEntry, nil
}

// Mode transition
func (r *Replica) ConvertToLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Mode == Leader {
		return
	}
	r.Mode = Leader
	r.timestamp = 0
	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()
	r.lsm = NewLSM()
}

func (r *Replica) ConvertToFollower() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Mode = Follower
	r.timestamp = 0
	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()
	r.lsm = NewLSM()
}

func (r *Replica) modePrefix() string {
	if r.Mode == Leader {
		return "Leader"
	}
	return "Follower"
}

func (r *Replica) ReceiveSnapshot(snapshot *Snapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()

	r.lsm.immutables = make([]*MemTable, len(snapshot.Tables))
	for i := range snapshot.Tables {
		r.lsm.immutables[i] = &snapshot.Tables[i]
	}

	// apply data from snapshot
	r.data = make(map[string]ReplicaData)
	for _, table := range snapshot.Tables {
		for _, logEntry := range table.data {
			r.ApplyLogEntry(logEntry)
		}
	}

	// log number of data entries after applying snapshot
	log.Printf("Replica %d applied snapshot resulting in %d data entries\n", r.Id, len(r.data))

	r.lsm.mem = NewMemTable()
}

func (r *Replica) GetSnapshot() *Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.lsm.mem.data) > 0 {
		r.lsm.flush()
	}

	r.lsm.mu.RLock()
	defer r.lsm.mu.RUnlock()

	snapshot := &Snapshot{
		Tables: make([]MemTable, len(r.lsm.immutables)),
	}

	for i, table := range r.lsm.immutables {
		snapshot.Tables[i] = *table
	}

	log.Println("Snapshot created with " + strconv.FormatInt(int64(len(snapshot.Tables)), 10) + " tables")

	return snapshot
}

// does NOT acquire lock. assumes caller has already acquired the lock.
func (r *Replica) ApplyLogEntry(logEntry ReplicaLog) {
	if logEntry.Action == ReplicaActionSet {
		if currentData, exists := r.data[logEntry.Key]; !exists || currentData.Timestamp < logEntry.Timestamp {
			data := ReplicaData{
				Value:     logEntry.Value,
				Timestamp: logEntry.Timestamp,
			}
			r.data[logEntry.Key] = data
		} else {
			log.Println("Key " + logEntry.Key + " already exists with a newer timestamp, ignoring snapshot entry")
		}
	} else if logEntry.Action == ReplicaActionDelete {
		if _, exists := r.data[logEntry.Key]; exists {
			delete(r.data, logEntry.Key)
		} else {
			log.Println("Key " + logEntry.Key + " not found for deletion in snapshot")
		}
	} else {
		log.Println("Unknown action in snapshot entry: " + logEntry.String())
	}
}
