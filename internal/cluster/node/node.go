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
		// TODO(discuss): ask mohammad when would I need IsAlive and ID?
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

        if err := encoder.Encode("ReplicaSet"); err != nil {
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

func (n *Node) getNodesContainingPartition(partitionId int) ([]controller.NodeMetadata, error) {
	// TODO
	return make([]controller.NodeMetadata, 0), nil
}

func (n *Node) loadConfig() error {
	// TODO
	return nil
}

func (n *Node) heartbeat() {
	// TODO
}
