package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/hash"
)

type Node struct {
	Id       int
	replicas map[int]*replica.Replica // partitionId, replica of that partiotionId
}

func NewNode(id int) Node {
	return Node{
		Id:       id,
		replicas: make(map[int]*replica.Replica),
	}
	// TODO: run node.start node in main.go file of container
}

func (n *Node) HandleSetFromLB(w http.ResponseWriter, r *http.Request) {
	// TODO(discuss): about these http methods
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		log.Printf("[node.HandleSetFromLB] Invalid method: %s from %s", r.Method, r.RemoteAddr)
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[node.HandleSetFromLB] Failed to read body from %s: %v", r.RemoteAddr, err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req SetRequestFromLB
	if err := json.Unmarshal(body, &req); err != nil {
		log.Printf("[node.HandleSetFromLB] Invalid JSON payload from %s: %v | Body: %s", r.RemoteAddr, err, string(body))
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		log.Printf("[node.HandleSetFromLB] Empty key from %s | Payload: %+v", r.RemoteAddr, req)
		http.Error(w, "Key cannot be empty", http.StatusBadRequest)
		return
	}

	// TODO
	partitionID := hash.HashKey(req.Key)
	err = n.set(partitionID, req.Key, req.Value)
	if err != nil {
		log.Printf("[node.HandleSetFromLB] failed to set key '%s' in partition %d: %v", req.Key, partitionID, err)
		http.Error(w, fmt.Sprintf("Failed to set key: %v", err), http.StatusInternalServerError)
		return
	}

	// TODO(discuss): discuss about this response with Mohammad
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (n *Node) set(partitionId int, timestamp int64, key, value string) error {
	// find the replica that has to store this key
	r, ok := n.replicas[partitionId]
	if !ok {
		return fmt.Errorf("[node.set] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	if r.Mode == replica.Follower {
	   return fmt.Errorf("node id: %v contains no leader for partition %v", n.Id, partitionId)
	}

	// TODO(me): append to WAL

	replicaLog, err := r.Set(key, value, timestamp)
	if err != nil {
		return fmt.Errorf("[node.set] failed to set(key, value) to partitionId: %v in nodeId: %v | err: %v", partitionId, n.Id, err)
	}

	err = n.broadcastToFollowers(replicaLog)
	if err != nil {
		// TODO(discuss): ask if this should be error or log
		return fmt.Errorf("[node.set] failed to broadcast set request to followes in nodeId: %v | err: %v", n.Id, err)
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
		return fmt.Errorf("[node.broadcastToFollowers] failed to marshal RequestToFollowerNodes: %v", err)
	}

	for _, fn := range followersNodes {
		go func(fn *controller.NodeMetadata) {
			maxRetries := 3
			for i := 0; i < maxRetries; i++ {
				// TODO(discuss): set this address
				url := fmt.Sprintf("http://%s/set/follower-node", fn.Address)

				resp, err := http.Post(url, "application/json", bytes.NewReader(bodyBytes))
				if err == nil && resp.StatusCode == http.StatusOK {
					log.Printf("[node.broadcastToFollowers] Successfully replicated to follower node %s", fn.Address)
					resp.Body.Close()
					return
				}

				if resp != nil {
					resp.Body.Close()
				}

				log.Printf("[node.broadcastToFollowers] Failed to replicate to follower node %s (attempt %d): %v", fn.Address, i+1, err)
				time.Sleep(1 * time.Second) // TODO(discuss): how many seconds we should wait for response? test it.
			}

			log.Printf("[node.broadcastToFollowers] Giving up on follower node %s after %d retries", fn.Address, maxRetries)
		}(fn)
	}

	return nil
}
