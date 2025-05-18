package node

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
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

func (n *Node) Start() error {
	err := n.loadConfig()
	if err != nil {
		return fmt.Errorf("[node.Start] can not load config due to: %v", err)
	}
	go n.heartbeat()

	n.replicasInitialization()
	go n.tcpListener("lb-requests-to-node"+strconv.Itoa(n.Id), n.lbConnectionHandler) // TODO: read about this address
	// go n.tcpListener("leader-requests-to-follower"+strconv.Itoa(n.Id), n.leaderNodeConnectionHandler) // TODO: read about this address
	// go n.tcpListener("follower-requests-to-leader"+strconv.Itoa(n.Id), n.followerNodeConnectionHandler) // TODO: read about this address
	// TODO: other tcp listeners
	return nil
}

func (n *Node) replicasInitialization() {
	// TODO
}

func (n *Node) tcpListener(address string, handler func(Message) Response) {
	ln, err := net.Listen("tcp", ":"+address)
	if err != nil {
		log.Printf("[node.tcpListener] Node failed to listen on address %s: %v", address, err)
		return
	}
	log.Printf("[node.tcpListener] Listening on address %s", address)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[node.tcpListener] Connection accept error: %v", err)
			continue
		}
		go func(conn net.Conn) {
			defer conn.Close()
			decoder := gob.NewDecoder(conn)
			encoder := gob.NewEncoder(conn)

			var msg Message
			if err := decoder.Decode(&msg); err != nil {
				log.Printf("[node.tcpListener] Failed to decode message: %v", err)
				return
			}

			resp := handler(msg)
			if err := encoder.Encode(resp); err != nil {
				log.Printf("[node.tcpListener] Failed to send response: %v", err)
			}
		}(conn)
	}
}

func (n *Node) lbConnectionHandler(msg Message) Response {
	switch msg.Type {
	case Set:
		err := n.set(msg.PartitionId, msg.Timestamp, msg.Key, msg.Value, replica.Leader)
		if err != nil {
			log.Printf("[node.lbConnectionHandler] failed to set key '%s' in partition %d: %v", msg.Key, msg.PartitionId, err)
			return Response{Error: err}
		}
		return Response{}
	case Get:
		val, err := n.get(msg.PartitionId, msg.Key)
		if err != nil {
			log.Printf("[node.lbConnectionHandler] failed to get key '%s' from partition %d: %v", msg.Key, msg.PartitionId, err)
			return Response{Error: err}
		}
		return Response{Value: val}
	case Delete:
		err := n.delete(msg.PartitionId, msg.Timestamp, msg.Key, replica.Leader)
		if err != nil {
			log.Printf("[node.lbConnectionHandler] failed to delete key '%s' from partition %d: %v", msg.Key, msg.PartitionId, err)
			return Response{Error: err}
		}
		return Response{}
	default:
		return Response{Error: fmt.Errorf("unknown message type")}
	}
}

// This function handels requests of some node containing a leader with some follower replica in this node
// func (n *Node) leaderNodeConnectionHandler(msg Message) Response {

// }

// This function sends requests a leader replica in this node to some follower replica another node
// func (n *Node) followerNodeConnectionHandler(msg Message) Response {

// }

func (n *Node) set(partitionId int, timestamp int64, key string, value string, replicaType replica.ReplicaType) error {
	r, ok := n.replicas[partitionId]
	if !ok {
		return fmt.Errorf("[node.set] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	if r.Mode != replicaType {
		return fmt.Errorf("[node.set] node id: %v contains no %v replica for partition %v", n.Id, replicaType, partitionId)
	}

	// TODO(me): append to WAL

	replicaLog, err := r.Set(key, value, timestamp)
	if err != nil {
		return fmt.Errorf("[node.set] failed to set(%v, %v) to partitionId: %v in nodeId: %v | err: %v", key, value, partitionId, n.Id, err)
	}

	if replicaType == replica.Leader {
		go n.broadcastToFollowers(replicaLog)
	}
	return nil
}

func (n *Node) get(partitionId int, key string) (string, error) {
	r, ok := n.replicas[partitionId]
	if !ok {
		return "", fmt.Errorf("[node.get] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	replicaLog, err := r.Get(key)
	if err != nil {
		return "", fmt.Errorf("[node.get] failed to get(%v) to partitionId: %v in nodeId: %v | err: %v", key, partitionId, n.Id, err)
	}

	return replicaLog.Value, nil
}

func (n *Node) delete(partitionId int, timestamp int64, key string, replicaType replica.ReplicaType) error {
	r, ok := n.replicas[partitionId]
	if !ok {
		return fmt.Errorf("[node.delete] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	if r.Mode != replicaType {
		return fmt.Errorf("[node.delete] node id: %v contains no %v replica for partition %v", n.Id, replicaType, partitionId)
	}

	// TODO(me): append to WAL

	replicaLog, err := r.Delete(key, timestamp)
	if err != nil {
		return fmt.Errorf("[node.delete] failed to delete(%v) from partitionId: %v in nodeId: %v | err: %v", key, partitionId, n.Id, err)
	}

	if replicaType == replica.Leader {
		go n.broadcastToFollowers(replicaLog)
	}
	return nil
}

// This function broadcase set/delete requests to all follower replicas
func (n *Node) broadcastToFollowers(replicaLog replica.ReplicaLog) {
	// followersNodes := controller.GetNodesContainingPartition(replicaLog.PartitionId)
	// reqBody := RequestToFollowerNodes{replicaLog}

	// bodyBytes, err := json.Marshal(reqBody)
	// if err != nil {
	// 	log.Printf("[node.broadcastToFollowers] failed to marshal RequestToFollowerNodes: %v", err)
	// 	return
	// }

	// for _, fn := range followersNodes {
	// 	go func(fn *controller.NodeMetadata) {
	// 		maxRetries := 3
	// 		for i := 0; i < maxRetries; i++ {
	// 			// TODO(discuss): set this address
	// 			url := fmt.Sprintf("http://%s/set/follower-node", fn.Address)

	// 			resp, err := http.Post(url, "application/json", bytes.NewReader(bodyBytes))
	// 			if err == nil && resp.StatusCode == http.StatusOK {
	// 				log.Printf("[node.broadcastToFollowers] Successfully replicated to follower node %s", fn.Address)
	// 				resp.Body.Close()
	// 				return
	// 			}

	// 			if resp != nil {
	// 				resp.Body.Close()
	// 			}

	// 			log.Printf("[node.broadcastToFollowers] Failed to replicate to follower node %s (attempt %d): %v", fn.Address, i+1, err)
	// 			time.Sleep(1 * time.Second) // TODO(discuss): how many seconds we should wait for response? test it.
	// 		}

	// 		log.Printf("[node.broadcastToFollowers] Giving up on follower node %s after %d retries", fn.Address, maxRetries)
	// 	}(fn)
	// }
}

func (n *Node) loadConfig() error {
	// TODO
	return nil
}

func (n *Node) heartbeat() {
	// TODO
}
