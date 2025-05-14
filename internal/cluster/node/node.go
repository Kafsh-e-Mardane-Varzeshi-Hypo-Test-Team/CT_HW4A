package node

import (
	"errors"
	"fmt"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
)

type Node struct {
	Id       int
	replicas map[int]replica.Replica // partitionId, replica of that partiotionId
}

type NodeResp struct {
	partitionId int
	// enum ok, nok
	// error
	replica.ReplicaResp
}

func NewNode(id int) Node {
	return Node{
		Id:       id,
		replicas: make(map[int]replica.Replica),
	}
}

func (n *Node) Set(key, value string) (NodeResp, error) {
	// TODO
	replica, ok := n.replicas[getReplicaId(key)]
	if !ok {
		return errors.New(fmt.Sprintf("node id: %v contains no partition containing key:%s", n.Id, key))
	}

	replicaResp, err := replica.Set(key, value)
	if err != nil {
		return err
	}
	if replica.isLeader {
		return broadcastSet(replicaResp)
	}
	return nil
}

func (n *Node) Get(key string) (NodeResp, error) {
	// TODO
	return NodeResp{}, nil
}

func (n *Node) Delete(key string) (NodeResp, error) {
	// TODO
	return NodeResp{}, nil
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

func getReplicaId(key string) int {
	// TODO
	return 5
}

func broadcastSet(replicaResp replica.ReplicaResp) error {
	// TODO
	GetPartition(id)
	{
		nodes []string
	}
	return nil
}