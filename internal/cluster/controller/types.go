package controller

type NodeStatus string

const (
	Up       NodeStatus = "Up"
	Down     NodeStatus = "Down"
	Creating NodeStatus = "Creating"
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
