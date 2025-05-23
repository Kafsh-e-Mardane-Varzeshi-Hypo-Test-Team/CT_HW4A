package node

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
	"github.com/gin-gonic/gin"
)

const (
	CONTROLLER_ADDRESS = "8000"
	HEARTBEAT_TIMER    = 2 * time.Second
)

type Node struct {
	Id        int
	replicas  map[int]*replica.Replica // partitionId, replica of that partiotionId
	ginEngine *gin.Engine
}

func NewNode(id int) Node {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	return Node{
		Id:        id,
		replicas:  make(map[int]*replica.Replica),
		ginEngine: router,
	}
	// TODO: run node.start node in main.go file of container
}

func (n *Node) Start() error {
	err := n.loadConfig()
	if err != nil {
		return fmt.Errorf("[node.Start] can not load config due to: %v", err)
	}
	go n.startHeartbeat(HEARTBEAT_TIMER)

	go n.tcpListener("follower-requests-to-leader"+strconv.Itoa(n.Id), n.nodeConnectionHandler) // TODO: read about this address
	// TODO: other tcp listeners
	return nil
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

// This function handels requests of some node containing a leader with some follower replica in this node
func (n *Node) nodeConnectionHandler(msg Message) Response {
	switch msg.Type {
	case Set:
		err := n.set(msg.PartitionId, msg.Timestamp, msg.Key, msg.Value, replica.Follower)
		if err != nil {
			log.Printf("[node.nodeConnectionHandler] failed to set key '%s' in partition %d: %v", msg.Key, msg.PartitionId, err)
			return Response{Error: err}
		}
		return Response{}
	case Delete:
		err := n.delete(msg.PartitionId, msg.Timestamp, msg.Key, replica.Follower)
		if err != nil {
			log.Printf("[node.nodeConnectionHandler] failed to delete key '%s' from partition %d: %v", msg.Key, msg.PartitionId, err)
			return Response{Error: err}
		}
		return Response{}
	default:
		return Response{Error: fmt.Errorf("unknown message type")}
	}
}

func (n *Node) set(partitionId int, timestamp int64, key string, value string, replicaType replica.ReplicaType) error {
	r, ok := n.replicas[partitionId]
	if !ok {
		return fmt.Errorf("[node.set] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	if r.Mode != replicaType {
		return fmt.Errorf("[node.set] node id: %v contains no %v replica for partition %v", n.Id, replicaType, partitionId)
	}

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
	msg := Message{
		PartitionId: replicaLog.PartitionId,
		Timestamp:   replicaLog.Timestamp,
		Key:         replicaLog.Key,
		Value:       replicaLog.Value,
	}

	if replicaLog.Action == replica.ReplicaActionSet {
		msg.Type = Set
	} else if replicaLog.Action == replica.ReplicaActionDelete {
		msg.Type = Delete
	}

	followersNodes, err := n.getNodesContainingPartition(replicaLog.PartitionId)
	if err != nil {
		log.Printf("[node.broadcastToFollowers] failed get nodes containing partitionId: %d from controller: %v", replicaLog.PartitionId, err)
	}

	for _, fn := range followersNodes {
		go n.replicateToFollower(fn, msg)
	}
}

func (n *Node) replicateToFollower(fn controller.NodeMetadata, msg Message) {
	maxRetries := 3
	retryDelay := 100 * time.Millisecond
	for i := 0; i < maxRetries; i++ {
		conn, err := net.Dial("tcp", ":"+fn.Address)
		if err != nil {
			log.Printf("[node.replicateToFollower] failed to connect to %s: %v", fn.Address, err)
			time.Sleep(retryDelay)
			continue
		}

		encoder := gob.NewEncoder(conn)
		decoder := gob.NewDecoder(conn)

		if err := encoder.Encode("Request-From-Node-Of-Leader-Partition"); err != nil {
			log.Printf("[node.replicateToFollower] failed to send request type: %v", err)
			conn.Close()
			time.Sleep(retryDelay)
			continue
		}

		if err := encoder.Encode(msg); err != nil {
			log.Printf("[node.replicateToFollower] failed to send message: %v", err)
			conn.Close()
			time.Sleep(retryDelay)
			continue
		}

		var resp Response
		if err := decoder.Decode(&resp); err != nil {
			log.Printf("[node.replicateToFollower] failed to decode response: %v", err)
			conn.Close()
			time.Sleep(retryDelay)
			continue
		}

		conn.Close()

		if resp.Error != nil {
			log.Printf("[node.replicateToFollower] follower at %s responded with error: %s", fn.Address, resp.Error)
			time.Sleep(retryDelay)
			continue
		}

		log.Printf("[node.replicateToFollower] successfully replicated to %s", fn.Address)
		return
	}

	log.Printf("[node.replicateToFollower] failed to replicate to follower at %s after %d retries", fn.Address, maxRetries)
}

// TODO: convert to http
func (n *Node) getNodesContainingPartition(partitionId int) ([]controller.NodeMetadata, error) {
	conn, err := net.Dial("tcp", ":"+CONTROLLER_ADDRESS)
	if err != nil {
		return nil, fmt.Errorf("[node.getNodesContainingPartition] failed to connect to controller at %s: %v", CONTROLLER_ADDRESS, err)
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

	if err := encoder.Encode("Get-Nodes-Containing-Partition"); err != nil {
		return nil, fmt.Errorf("[node.getNodesContainingPartition] failed to encode request type: %v", err)
	}

	if err := encoder.Encode(partitionId); err != nil {
		return nil, fmt.Errorf("[node.getNodesContainingPartition] failed to encode partition ID: %v", err)
	}

	var nodes []controller.NodeMetadata
	if err := decoder.Decode(&nodes); err != nil {
		return nil, fmt.Errorf("[node.getNodesContainingPartition] failed to decode response: %v", err)
	}

	return nodes, nil
}

func (n *Node) loadConfig() error {
	// TODO
	return nil
}

func (n *Node) startHeartbeat(interval time.Duration) {
	go func() {
		for {
			err := n.sendHeartbeat()
			if err != nil {
				log.Printf("[node.startHeartbeat] failed to send heartbeat: %v", err)
			}
			time.Sleep(interval)
		}
	}()
}

// TODO: convert to http
func (n *Node) sendHeartbeat() error {
	conn, err := net.Dial("tcp", CONTROLLER_ADDRESS)
	if err != nil {
		return fmt.Errorf("failed to connect to controller at %s: %v", CONTROLLER_ADDRESS, err)
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)

	if err := encoder.Encode("Heartbeat-From-Node"); err != nil {
		return fmt.Errorf("failed to encode heartbeat message type: %v", err)
	}

	hb := Heartbeat{
		NodeId:    n.Id,
		Timestamp: time.Now().Unix(),
	}
	if err := encoder.Encode(hb); err != nil {
		return fmt.Errorf("failed to encode heartbeat payload: %v", err)
	}

	return nil
}
