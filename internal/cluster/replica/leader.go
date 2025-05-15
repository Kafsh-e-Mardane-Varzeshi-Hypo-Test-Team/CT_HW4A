package replica

import (
	"log"
	"strconv"
	"sync"
)

type LeaderReplica struct {
	Id          int
	NodeId      int
	PartitionId int
	data        map[string]ReplicaData
	logs        []ReplicaLog
	timestamp   int64
	mu          sync.RWMutex
}

func NewMasterReplica(id, nodeId, partitionId int) *LeaderReplica {
	return &LeaderReplica{
		Id:          id,
		NodeId:      nodeId,
		PartitionId: partitionId,
		data:        make(map[string]ReplicaData),
		logs:        []ReplicaLog{},
		timestamp:   0,
		mu:          sync.RWMutex{},
	}
}

// error is nil for now. no failures expected
func (r *LeaderReplica) Set(key, value string) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.data[key] = ReplicaData{
		Value:     value,
		Timestamp: r.timestamp,
	}

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

	log.Println("LeaderSetKey: " + l.String())
	return l, nil
}

// Get retrieves the value for the given key.
// If the key does not exist, it returns an error.
func (r *LeaderReplica) Get(key string) (ReplicaLog, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data, exists := r.data[key]
	if !exists {
		err := &KeyNotFoundError{
			key: key,
		}
		log.Println(err.Error())
		return ReplicaLog{}, err
	}

	l := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionGet,
		Timestamp:   r.timestamp,
		Key:         key,
		Value:       data.Value,
	}

	log.Println("LeaderGetKey: " + l.String())
	return l, nil
}

func (r *LeaderReplica) Delete(key string) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, exists := r.data[key]
	if !exists {
		err := &KeyNotFoundError{
			key: key,
		}
		log.Println(err.Error())
		return ReplicaLog{}, err
	}

	delete(r.data, key)

	l := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionDelete,
		Timestamp:   r.timestamp,
		Key:         key,
		Value:       "",
	}

	r.logs = append(r.logs, l)

	r.timestamp++

	log.Println("LeaderDeleteKey: " + l.String())
	return l, nil
}

func (r *LeaderReplica) GetLogsSince(timestamp int64) []ReplicaLog {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// binary search for start index
	start := 0
	end := len(r.logs) - 1
	for start <= end {
		mid := (start + end) / 2
		if r.logs[mid].Timestamp < timestamp {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}

	// start2 is now the first index where the timestamp is greater than or equal to timestampEnd
	if start >= len(r.logs) {
		log.Println("no logs found since timestamp " + strconv.FormatInt(timestamp, 10))
		return []ReplicaLog{}
	}

	logs := r.logs[start:]

	log.Println("returned " + strconv.Itoa(len(logs)) + " logs since timestamp " + strconv.FormatInt(timestamp, 10))
	return logs
}

// GetLogs returns the logs between the given timestamps.
// Start timestamp is inclusive and end timestamp is exclusive.
func (r *LeaderReplica) GetLogs(timestampStart, timestampEnd int64) []ReplicaLog {
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

	// binary search for end index (exclusive)
	start2 := 0
	end2 := len(r.logs) - 1
	for start2 <= end2 {
		mid := (start2 + end2) / 2
		if r.logs[mid].Timestamp < timestampEnd {
			start2 = mid + 1
		} else {
			end2 = mid - 1
		}
	}

	// start2 is now the first index where the timestamp is greater than or equal to timestampEnd
	if start1 >= len(r.logs) || start2 <= start1 {
		log.Println("no logs found between timestamps " + strconv.FormatInt(timestampStart, 10) + " and " + strconv.FormatInt(timestampEnd, 10))
		return []ReplicaLog{}
	}

	logs := r.logs[start1:start2]

	log.Println("returned " + strconv.Itoa(len(logs)) + " logs from timestamp " + strconv.FormatInt(timestampStart, 10) + " before " + strconv.FormatInt(timestampEnd, 10))
	return logs
}
