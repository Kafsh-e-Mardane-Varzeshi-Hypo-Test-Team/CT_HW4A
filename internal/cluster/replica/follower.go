package replica

import (
	"log"
	"strconv"
	"sync"
)

type FollowerReplica struct {
	Id          int
	NodeId      int
	PartitionId int
	data        map[string]ReplicaData
	logs        []ReplicaLog
	mu          sync.RWMutex
}

func NewFollowerReplica(id, nodeId, partitionId int) *FollowerReplica {
	return &FollowerReplica{
		Id:          id,
		NodeId:      nodeId,
		PartitionId: partitionId,
		data:        make(map[string]ReplicaData),
		logs:        []ReplicaLog{},
		mu:          sync.RWMutex{},
	}
}

func (r *FollowerReplica) Set(key, value string, timestamp int64) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if data, exists := r.data[key]; exists {
		// TODO: check if timestamp collision can happen
		if data.Timestamp >= timestamp {
			err := &OldSetRequestError{
				key:              key,
				oldTimestamp:     data.Timestamp,
				oldValue:         data.Value,
				requestTimestamp: timestamp,
				newValue:         value,
			}
			log.Println(err.Error())
			return ReplicaLog{}, err
		}
	}

	r.data[key] = ReplicaData{
		Value:     value,
		Timestamp: timestamp,
	}

	l := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		ReplicaId:   r.Id,
		Action:      ReplicaActionSet,
		Timestamp:   timestamp,
		Key:         key,
		Value:       value,
	}

	r.logs = append(r.logs, l)

	log.Println("FollowerSetKey: " + l.String())
	return l, nil
}

// Get retrieves the value for the given key.
// If the key does not exist, it returns an error.
// -1 is used as a placeholder for the timestamp in the log entry.
func (r *FollowerReplica) Get(key string) (ReplicaLog, error) {
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
		Timestamp:   -1,
		Key:         key,
		Value:       data.Value,
	}

	log.Println("FollowerGetKey: " + l.String())
	return l, nil
}

func (r *FollowerReplica) Delete(key string, timestamp int64) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	data, exists := r.data[key]
	if !exists {
		err := &KeyNotFoundError{
			key: key,
		}
		log.Println(err.Error())
		return ReplicaLog{}, err
	}

	if data.Timestamp >= timestamp {
		err := &OldDeleteRequestError{
			key:              key,
			oldValue:         data.Value,
			oldTimestamp:     data.Timestamp,
			requestTimestamp: timestamp,
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
		Timestamp:   timestamp,
		Key:         key,
		Value:       "",
	}

	r.logs = append(r.logs, l)

	log.Println("FollowerDeleteKey: " + l.String())
	return l, nil
}

func (r *FollowerReplica) GetLogsSince(timestamp int64) []ReplicaLog {
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
func (r *FollowerReplica) GetLogs(timestampStart, timestampEnd int64) []ReplicaLog {
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

// TODO: requests must be blocked until the leader replica is ready, then send the request to the leader replica
func (r *FollowerReplica) ConvertToLeader() *LeaderReplica {
	r.mu.Lock()
	defer r.mu.Unlock()

	leader := NewLeaderReplica(r.Id, r.NodeId, r.PartitionId)
	leader.data = r.data
	leader.logs = r.logs
	leader.timestamp = r.logs[len(r.logs)-1].Timestamp
	return leader
}
