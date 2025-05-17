package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
)

type Node struct {
	Id       int
	replicas map[int]*replica.LeaderReplica // partitionId, replica of that partiotionId
}

func NewNode(id int) Node {
	return Node{
		Id:       id,
		replicas: make(map[int]*replica.LeaderReplica),
	}
}

func (n *Node) SetInLeaderReplica(partiotionId int, key, value string) error {
	// find the replica that has to store this key
	replica, ok := n.replicas[partiotionId]
	if !ok {
		return fmt.Errorf("node id: %v contains no partition containing key:%s", n.Id, key)
	}

	// if we have only one replica struct (not leader and follower):
	// if !partition.IsLeader {
	//    return fmt.Errorf("node id: %v contains no leader for partition %v", n.Id, partitionID)
	// }

	// TODO: append to WAL

	// set key, value
	replicaLog, err := replica.Set(key, value)
	if err != nil {
		return fmt.Errorf("failed to set(key, value) to partitionId: %v in nodeId: %v, err: %v", partiotionId, n.Id, err)
	}

	err = n.broadcastToFollowers(replicaLog)
	if err != nil {
		return fmt.Errorf("failed to broadcast set request to followes in nodeId: %v, err: %v", n.Id, err)
	}

	return nil
}

func (n *Node) Get(key string) error {
	// TODO
	return nil
}

func (n *Node) Delete(key string) error {
	// TODO
	return nil
}

func Start() {
	replicasInitialization()
	tcpInitialization()
	go heartbeat()

}

func replicasInitialization() {
	// TODO
}

func tcpInitialization() {
	// TODO
}

func heartbeat() {
	// TODO
}

func (n *Node) broadcastToFollowers(replicaLog replica.ReplicaLog) error {
	followersNodes := controller.GetNodesContainingPartition(replicaLog.PartitionId)
    reqBody := RequestToFollowerNodes{replicaLog}

    bodyBytes, err := json.Marshal(reqBody)
    if err != nil {
		return fmt.Errorf("failed to marshal RequestToFollowerNodes: %v", err)
    }

	for _, fn := range followersNodes {
        go func(fn *controller.NodeMetadata) {
            maxRetries := 3
            for i := 0; i < maxRetries; i++ {
				// TODO: fix address
                url := fmt.Sprintf("http://%s/set/follower-node", fn.Address)

                resp, err := http.Post(url, "application/json", bytes.NewReader(bodyBytes))
                if err == nil && resp.StatusCode == http.StatusOK {
                    log.Printf("Broadcast Successfully replicated to follower node %s", fn.Address)
                    resp.Body.Close()
                    return
                }

                if resp != nil {
                    resp.Body.Close()
                }

                log.Printf("Broadcast Failed to replicate to follower node %s (attempt %d): %v", fn.Address, i+1, err)
                time.Sleep(1 * time.Second) // TODO: fix this
            }

            log.Printf("Broadcast Giving up on follower node %s after %d retries", fn.Address, maxRetries)
        }(fn)
    }

	return nil
}
