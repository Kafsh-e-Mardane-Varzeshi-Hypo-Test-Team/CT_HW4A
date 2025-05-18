package node

import (
	"fmt"
	"log"
	"net"
	"strconv"

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

func (n *Node) HandleSetFromLB() {
	// TODO: get req from lb using some connection protocol
	req := SetRequest{
		Key:   "something",
		Value: "something else",
	}

	partitionId := hash.HashKey(req.Key)
	err := n.setInLeader(partitionId, req.Key, req.Value)
	if err != nil {
		log.Printf("[node.HandleSetFromLB] failed to set key '%s' in partition %d: %v", req.Key, partitionId, err)
		// TODO: nok resp to lb
		return
	}

	// TODO: ok resp to lb
}

func (n *Node) HandleSetFromSomeNode() {
	// TODO: get req from some node using some connection protocol
	req := SetRequest{
		timestamp: 1,
		Key:       "something",
		Value:     "something else",
	}

	partitionId := hash.HashKey(req.Key)
	err := n.setInFollower(partitionId, req.timestamp, req.Key, req.Value)
	if err != nil {
		log.Printf("[node.HandleSetFromSomeNode] failed to set key '%s' in partition %d: %v", req.Key, partitionId, err)
	}
}

func (n *Node) setInLeader(partitionId int, key, value string) error {
	// find the replica that has to store this key
	r, ok := n.replicas[partitionId]
	if !ok {
		return fmt.Errorf("[node.setInLeader] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	if r.Mode == replica.Follower {
		return fmt.Errorf("[node.setInLeader] node id: %v contains no leader for partition %v", n.Id, partitionId)
	}

	// TODO(me): append to WAL

	replicaLog, err := r.Set(key, value, -1)
	if err != nil {
		return fmt.Errorf("[node.setInLeader] failed to set(key, value) to partitionId: %v in nodeId: %v | err: %v", partitionId, n.Id, err)
	}

	go n.broadcastToFollowers(replicaLog)
	return nil
}

func (n *Node) setInFollower(partitionId int, timestamp int64, key string, value string) error {
	// find the replica that has to store this key
	r, ok := n.replicas[partitionId]
	if !ok {
		return fmt.Errorf("[node.setInFollower] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	if r.Mode == replica.Leader {
		return fmt.Errorf("[node.setInFollower] node id: %v contains no follower for partition %v", n.Id, partitionId)
	}

	// TODO(me): append to WAL

	_, err := r.Set(key, value, timestamp)
	if err != nil {
		return fmt.Errorf("[node.setInFollower] failed to set(key, value) to partitionId: %v in nodeId: %v | err: %v", partitionId, n.Id, err)
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

func (n *Node) Start() error {
	err := n.loadConfig()
	if err != nil {
		return fmt.Errorf("[node.Start] can not load config due to: %v", err)
	}
	go n.heartbeat()
	
	n.replicasInitialization()
	go n.tcpListener("lb-node-" + strconv.Itoa(n.Id), n.lbHandler)
	// TODO: other tcp listeners
	return nil
}

func (n *Node) loadConfig() error {
	// TODO
	return nil
}

func (n *Node) tcpListener(address string, handler func(net.Conn)) {
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
		go handler(conn)
	}
}

func (n *Node) lbHandler(conn net.Conn) {
	// TODO
}

func (n *Node) replicasInitialization() {
	// TODO
}

func (n *Node) heartbeat() {
	// TODO
}

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
