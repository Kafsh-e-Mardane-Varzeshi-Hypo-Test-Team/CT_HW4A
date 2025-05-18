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
	logs      []ReplicaLog
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
		logs:        []ReplicaLog{},
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

	r.logs = append(r.logs, logEntry)
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

	r.logs = append(r.logs, logEntry)
	log.Println(r.modePrefix() + "DeleteKey: " + logEntry.String())
	return logEntry, nil
}

// Logs filtering helpers
func (r *Replica) DropLogsBefore(timestamp int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx := r.searchLogs(timestamp)
	if idx >= len(r.logs) {
		r.logs = []ReplicaLog{}
		log.Println("no logs found since timestamp " + strconv.FormatInt(timestamp, 10) + ", dropped all logs")
		return
	}

	r.logs = r.logs[idx:]
	log.Println("kept " + strconv.Itoa(len(r.logs)) + " logs since timestamp " + strconv.FormatInt(timestamp, 10))
}

func (r *Replica) GetLogsSince(timestamp int64) []ReplicaLog {
	r.mu.RLock()
	defer r.mu.RUnlock()

	idx := r.searchLogs(timestamp)
	if idx >= len(r.logs) {
		log.Println("no logs found since timestamp " + strconv.FormatInt(timestamp, 10))
		return []ReplicaLog{}
	}

	logs := r.logs[idx:]
	log.Println("returned " + strconv.Itoa(len(logs)) + " logs since timestamp " + strconv.FormatInt(timestamp, 10))
	return logs
}

// GetLogs returns the logs between the given timestamps.
// Start timestamp is inclusive and end timestamp is exclusive.
func (r *Replica) GetLogs(start, end int64) []ReplicaLog {
	r.mu.RLock()
	defer r.mu.RUnlock()

	startIdx := r.searchLogs(start)
	endIdx := r.searchLogs(end)

	if startIdx >= len(r.logs) || endIdx <= startIdx {
		log.Println("no logs found between timestamps " + strconv.FormatInt(start, 10) + " and " + strconv.FormatInt(end, 10))
		return []ReplicaLog{}
	}

	logs := r.logs[startIdx:endIdx]
	log.Println("returned " + strconv.Itoa(len(logs)) + " logs from timestamp " + strconv.FormatInt(start, 10) + " before " + strconv.FormatInt(end, 10))
	return logs
}

// Mode transition
func (r *Replica) ConvertToLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Mode == Leader {
		return
	}
	r.Mode = Leader
	if len(r.logs) > 0 {
		r.timestamp = r.logs[len(r.logs)-1].Timestamp + 1
	}
}

func (r *Replica) ConvertToFollower() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Mode = Follower
}

// Binary search for the first log entry with a timestamp >= timestamp
func (r *Replica) searchLogs(timestamp int64) int {
	start, end := 0, len(r.logs)-1
	for start <= end {
		mid := (start + end) / 2
		if r.logs[mid].Timestamp < timestamp {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}
	return start
}

func (r *Replica) modePrefix() string {
	if r.Mode == Leader {
		return "Leader"
	}
	return "Follower"
}
