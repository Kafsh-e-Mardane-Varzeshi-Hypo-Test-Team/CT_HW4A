package replica

import (
	"errors"
	"sync"
)

type ReplicaAction int

const (
	ReplicaActionSet = iota
	ReplicaActionGet
	ReplicaActionDelete
)

type ReplicaLog struct {
	PartitionId int
	NodeId      int
	ReplicaId   int
	Action      ReplicaAction
	Timestamp   int64
	Key         string
	Value       string
}

type Replica struct {
	Id          int
	NodeId      int
	PartitionId int
	IsMaster    bool
	data        map[string]string
	logs        []ReplicaLog
	timestamp   int64
	mu          sync.RWMutex
}

func NewReplica(id, nodeId, partitionId int, isMaster bool) *Replica {
	return &Replica{
		Id:          id,
		NodeId:      nodeId,
		PartitionId: partitionId,
		IsMaster:    isMaster,
		data:        make(map[string]string),
		logs:        []ReplicaLog{},
		timestamp:   0,
		mu:          sync.RWMutex{},
	}
}

func (r *Replica) Set(key, value string) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.data[key] = value

	log := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionSet,
		Timestamp:   r.timestamp,
		Key:         key,
		Value:       value,
	}

	r.logs = append(r.logs, log)

	r.timestamp++

	return log, nil
}

func (r *Replica) Get(key string) (ReplicaLog, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	value, exists := r.data[key]
	if !exists {
		return ReplicaLog{}, errors.New("key not found")
	}

	log := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionGet,
		Timestamp:   r.timestamp,
		Key:         key,
		Value:       value,
	}

	return log, nil
}

func (r *Replica) Delete(key string) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	value, exists := r.data[key]
	if !exists {
		return ReplicaLog{}, errors.New("key not found")
	}

	delete(r.data, key)

	log := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionDelete,
		Timestamp:   r.timestamp,
		Key:         key,
		Value:       value,
	}

	r.logs = append(r.logs, log)

	r.timestamp++

	return log, nil
}
