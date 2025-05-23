package controller

import "time"

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
	ID          int        `json:"id"`
	HttpAddress string     `json:"http"`
	TcpAddress  string     `json:"tcp"`
	Status      NodeStatus `json:"status"`
	lastSeen    time.Time
}

type PartitionMetadata struct {
	PartitionID int   `json:"partitionId"`
	Leader      int   `json:"leader"`
	Replicas    []int `json:"replicas"`
}
