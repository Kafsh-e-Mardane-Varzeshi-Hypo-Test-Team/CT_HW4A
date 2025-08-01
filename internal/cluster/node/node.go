package node

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/replica"
	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	REQUEST_TIMEOUT = 2 * time.Second
	HEARTBEAT_TIMER = 2
	HTTP_ADDRESS    = "0.0.0.0:8000"
	TCP_ADDRESS     = "0.0.0.0:9000"
)

type Node struct {
	Id       int
	replicas map[int]*replica.Replica // partitionId, replica of that partiotionId

	replicasMapMutex sync.RWMutex
	ginEngine        *gin.Engine
	httpClient       *http.Client
	etcdClient       *clientv3.Client
}

func NewNode(id int, endpoints []string) *Node {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Printf("[node.NewNode] Failed to create etcd client: %v", err)
	}

	return &Node{
		Id:               id,
		replicas:         make(map[int]*replica.Replica),
		replicasMapMutex: sync.RWMutex{},
		ginEngine:        router,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		},
		etcdClient: cli,
	}
}

func (n *Node) Start() {
	defer n.etcdClient.Close()

	log.Println("[node.Start] Attempting to create etcd session...")
	session, err := concurrency.NewSession(n.etcdClient, concurrency.WithTTL(HEARTBEAT_TIMER))
	if err != nil {
		log.Fatalf("[node.Start] ETCD SESSION FAILED: %v", err)
	}
	log.Println("[node.Start] etcd session created successfully!")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	n.etcdClient.Put(ctx, fmt.Sprintf("nodes/active/%d", n.Id), "baghojenanash", clientv3.WithLease(session.Lease()))
	defer cancel()

	go n.tcpListener(n.nodeConnectionHandler)

	n.setupRoutes()
	log.Printf("Starting HTTP server on %s...", HTTP_ADDRESS)
	if err := n.ginEngine.Run(HTTP_ADDRESS); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func (n *Node) tcpListener(handler func(Message) Response) {
	ln, err := net.Listen("tcp", TCP_ADDRESS)
	if err != nil {
		log.Fatalf("[node.tcpListener] Node failed to listen on address %s: %v", TCP_ADDRESS, err)
	}
	log.Printf("[node.tcpListener] Listening on address %s", TCP_ADDRESS)

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
				return
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
	case Snapshot:
		r, exists := n.replicas[msg.PartitionId]
		if !exists {
			n.replicasMapMutex.Lock()
			r = replica.NewReplica(n.Id, msg.PartitionId, replica.Follower)
			n.replicas[msg.PartitionId] = r
			n.replicasMapMutex.Unlock()
		}
		r.ReceiveSnapshot(&msg.Snapshot)
		log.Printf("[nodeConnectionHandler] Received snapshot for partition %d", msg.PartitionId)
		return Response{}
	default:
		return Response{Error: fmt.Errorf("unknown message type")}
	}
}

func (n *Node) set(partitionId int, timestamp int64, key string, value string, replicaType replica.ReplicaType) error {
	n.replicasMapMutex.RLock()
	r, ok := n.replicas[partitionId]
	n.replicasMapMutex.RUnlock()
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
	n.replicasMapMutex.RLock()
	r, ok := n.replicas[partitionId]
	n.replicasMapMutex.RUnlock()
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
	n.replicasMapMutex.RLock()
	r, ok := n.replicas[partitionId]
	n.replicasMapMutex.RUnlock()
	if !ok {
		return fmt.Errorf("[node.delete] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	if r.Mode != replicaType {
		return fmt.Errorf("[node.delete] node id: %v contains no %v replica for partition %v", n.Id, replicaType, partitionId)
	}

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

	followersNodesAddresses, err := n.getNodesContainingPartition(replicaLog.PartitionId)
	if err != nil {
		log.Printf("[node.broadcastToFollowers] failed get nodes containing partitionId: %d from controller: %v", replicaLog.PartitionId, err)
	}

	for _, address := range followersNodesAddresses {
		go n.replicateToFollower(address, msg)
	}
}

func (n *Node) replicateToFollower(address string, msg Message) error {
	maxRetries := 3
	retryDelay := 100 * time.Millisecond
	for i := 0; i < maxRetries; i++ {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Printf("[node.replicateToFollower] failed to connect to %s: %v", address, err)
			time.Sleep(retryDelay)
			continue
		}

		encoder := gob.NewEncoder(conn)
		decoder := gob.NewDecoder(conn)

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
			log.Printf("[node.replicateToFollower] follower at %s responded with error: %s", address, resp.Error)
			time.Sleep(retryDelay)
			continue
		}

		log.Printf("[node.replicateToFollower] successfully replicated to %s", address)
		return nil
	}

	log.Printf("[node.replicateToFollower] failed to replicate to follower at %s after %d retries", address, maxRetries)
	return fmt.Errorf("failed to replicate to follower at %s after %d retries", address, maxRetries)
}

func (n *Node) sendSnapshotToNode(partitionId int, address string) error {
	n.replicasMapMutex.RLock()
	replica, exists := n.replicas[partitionId]
	if !exists {
		return fmt.Errorf("[node.sendSnapshotToNode] partition %d not found", partitionId)
	}

	snapshot := replica.GetSnapshot()
	if snapshot == nil {
		return fmt.Errorf("[node.sendSnapshotToNode] failed to get snapshot for partition %d", partitionId)
	}
	n.replicasMapMutex.RUnlock()

	msg := Message{
		Type:        Snapshot,
		PartitionId: replica.PartitionId,
		Snapshot:    *snapshot,
	}

	err := n.replicateToFollower(address, msg)

	if err != nil {
		return err
	}
	log.Printf("[node.SendSnapshotToNode] successfully sent snapshot of partition %d to %s", partitionId, address)
	return nil
}
