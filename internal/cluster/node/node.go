package node

import (
	"fmt"

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
		return err
	}

	// TODO: broadcast to followers
	err = n.broadcastToFollowers(replicaLog)
	if err != nil {
		return err
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
	// TODO
	return nil
}
