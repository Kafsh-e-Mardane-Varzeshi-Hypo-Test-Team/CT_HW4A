package controller

type NodeStatus string

const (
	Creating    NodeStatus = "creating"
	Configuring NodeStatus = "configuring"
	Syncing     NodeStatus = "syncing"
	Ready       NodeStatus = "ready"
	Alive       NodeStatus = "alive"
	Failed      NodeStatus = "failed"
)

type NodeMetadata struct {
	ID      int        `json:"id"`
	Address string     `json:"address"`
	Status  NodeStatus `json:"status"`
}

type PartitionMetadata struct {
	PartitionID int   `json:"partitionId"`
	Leader      int   `json:"leader"`
	Replicas    []int `json:"replicas"`
}
