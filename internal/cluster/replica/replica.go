package replica

import (
	"errors"
	"log"
	"strconv"
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

	l := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionSet,
		Timestamp:   r.timestamp,
		Key:         key,
		Value:       value,
	}

	r.logs = append(r.logs, l)

	r.timestamp++

	log.Println("key \"" + key + "\" set to \"" + value + "\"")
	return l, nil
}

func (r *Replica) Get(key string) (ReplicaLog, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	value, exists := r.data[key]
	if !exists {
		log.Println("key \"" + key + "\" not found")
		return ReplicaLog{}, errors.New("key \"" + key + "\" not found")
	}

	l := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionGet,
		Timestamp:   r.timestamp,
		Key:         key,
		Value:       value,
	}

	log.Println("key \"" + key + "\" found with value \"" + value + "\"")
	return l, nil
}

func (r *Replica) Delete(key string) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	value, exists := r.data[key]
	if !exists {
		log.Println("key \"" + key + "\" not found")
		return ReplicaLog{}, errors.New("key \"" + key + "\" not found")
	}

	delete(r.data, key)

	l := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionDelete,
		Timestamp:   r.timestamp,
		Key:         key,
		Value:       value,
	}

	r.logs = append(r.logs, l)

	r.timestamp++

	log.Println("key \"" + key + "\" deleted with value \"" + value + "\"")
	return l, nil
}

func (r *Replica) GetLogs(timestampStart, timestampEnd int64) []ReplicaLog {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// binary search for start1 index
	start1 := 0
	end1 := len(r.logs) - 1
	for start1 <= end1 {
		mid := (start1 + end1) / 2
		if r.logs[mid].Timestamp < timestampStart {
			start1 = mid + 1
		} else {
			end1 = mid - 1
		}
	}

	// binary search for end index
	start2 := 0
	end2 := len(r.logs) - 1
	for start2 <= end2 {
		mid := (start2 + end2) / 2
		if r.logs[mid].Timestamp <= timestampEnd {
			start2 = mid + 1
		} else {
			end2 = mid - 1
		}
	}
	// collect logs in the range
	logs := r.logs[start1:start2]

	return logs
}

func (l *ReplicaLog) String() string {
	action := ""
	switch l.Action {
	case ReplicaActionSet:
		action = "SET"
	case ReplicaActionGet:
		action = "GET"
	case ReplicaActionDelete:
		action = "DELETE"
	}
	return "ReplicaLog{" +
		"PartitionId: " + strconv.FormatInt(int64(l.PartitionId), 10) +
		", NodeId: " + strconv.FormatInt(int64(l.NodeId), 10) +
		", ReplicaId: " + strconv.FormatInt(int64(l.ReplicaId), 10) +
		", Action: " + action +
		", Timestamp: " + strconv.FormatInt(l.Timestamp, 10) +
		", Key: " + l.Key +
		", Value: " + l.Value +
		"}"
}
