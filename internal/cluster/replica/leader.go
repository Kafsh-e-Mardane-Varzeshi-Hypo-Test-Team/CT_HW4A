package replica

// import (
// 	"errors"
// 	"log"
// 	"strconv"
// 	"sync"
// )

// type MasterReplica struct {
// 	Id          int
// 	NodeId      int
// 	PartitionId int
// 	data        map[string]ReplicaData
// 	logs        []ReplicaLog
// 	timestamp   int64
// 	mu          sync.RWMutex
// }

// func NewMasterReplica(id, nodeId, partitionId int) *MasterReplica {
// 	return &MasterReplica{
// 		Id:          id,
// 		NodeId:      nodeId,
// 		PartitionId: partitionId,
// 		data:        make(map[string]ReplicaData),
// 		logs:        []ReplicaLog{},
// 		timestamp:   0,
// 		mu:          sync.RWMutex{},
// 	}
// }

// func (r *MasterReplica) Set(key, value string) (ReplicaLog, error) {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()

// 	r.data[key] = value

// 	l := ReplicaLog{
// 		PartitionId: r.PartitionId,
// 		NodeId:      r.NodeId,
// 		ReplicaId:   r.Id,
// 		Action:      ReplicaActionSet,
// 		Timestamp:   r.timestamp,
// 		Key:         key,
// 		Value:       value,
// 	}

// 	r.logs = append(r.logs, l)

// 	r.timestamp++

// 	log.Println("key \"" + key + "\" set to \"" + value + "\"")
// 	return l, nil
// }

// func (r *MasterReplica) Get(key string) (ReplicaLog, error) {
// 	r.mu.RLock()
// 	defer r.mu.RUnlock()

// 	value, exists := r.data[key]
// 	if !exists {
// 		log.Println("key \"" + key + "\" not found")
// 		return ReplicaLog{}, errors.New("key \"" + key + "\" not found")
// 	}

// 	l := ReplicaLog{
// 		PartitionId: r.PartitionId,
// 		NodeId:      r.NodeId,
// 		ReplicaId:   r.Id,
// 		Action:      ReplicaActionGet,
// 		Timestamp:   r.timestamp,
// 		Key:         key,
// 		Value:       value,
// 	}

// 	log.Println("key \"" + key + "\" found with value \"" + value + "\"")
// 	return l, nil
// }

// func (r *MasterReplica) Delete(key string) (ReplicaLog, error) {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()

// 	value, exists := r.data[key]
// 	if !exists {
// 		log.Println("key \"" + key + "\" not found")
// 		return ReplicaLog{}, errors.New("key \"" + key + "\" not found")
// 	}

// 	delete(r.data, key)

// 	l := ReplicaLog{
// 		PartitionId: r.PartitionId,
// 		NodeId:      r.NodeId,
// 		ReplicaId:   r.Id,
// 		Action:      ReplicaActionDelete,
// 		Timestamp:   r.timestamp,
// 		Key:         key,
// 		Value:       value,
// 	}

// 	r.logs = append(r.logs, l)

// 	r.timestamp++

// 	log.Println("key \"" + key + "\" deleted with value \"" + value + "\"")
// 	return l, nil
// }

// func (r *MasterReplica) GetLogsSince(timestamp int64) []ReplicaLog {
// 	return r.GetLogs(timestamp, r.timestamp)
// }

// // GetLogs returns the logs between the given timestamps.
// // Start timestamp is inclusive and end timestamp is exclusive.
// func (r *MasterReplica) GetLogs(timestampStart, timestampEnd int64) []ReplicaLog {
// 	r.mu.RLock()
// 	defer r.mu.RUnlock()

// 	// binary search for start1 index
// 	start1 := 0
// 	end1 := len(r.logs) - 1
// 	for start1 <= end1 {
// 		mid := (start1 + end1) / 2
// 		if r.logs[mid].Timestamp < timestampStart {
// 			start1 = mid + 1
// 		} else {
// 			end1 = mid - 1
// 		}
// 	}

// 	// binary search for end index (exclusive)
// 	start2 := 0
// 	end2 := len(r.logs) - 1
// 	for start2 <= end2 {
// 		mid := (start2 + end2) / 2
// 		if r.logs[mid].Timestamp < timestampEnd {
// 			start2 = mid + 1
// 		} else {
// 			end2 = mid - 1
// 		}
// 	}

// 	// start2 is now the first index where the timestamp is greater than or equal to timestampEnd
// 	if start1 >= len(r.logs) || start2 <= start1 {
// 		log.Println("no logs found between timestamps " + strconv.FormatInt(timestampStart, 10) + " and " + strconv.FormatInt(timestampEnd, 10))
// 		return []ReplicaLog{}
// 	}

// 	logs := r.logs[start1:start2]

// 	log.Println("returned " + strconv.Itoa(len(logs)) + " logs from timestamp " + strconv.FormatInt(timestampStart, 10) + " before " + strconv.FormatInt(timestampEnd, 10))
// 	return logs
// }
