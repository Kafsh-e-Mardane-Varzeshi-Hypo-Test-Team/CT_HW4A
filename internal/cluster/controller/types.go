package controller

type NodeMetadata struct {
	ID      int `json:"id"`
	Address string `json:"address"`
	IsAlive bool   `json:"isAlive"`
}

type PartitionMetadata struct {
	PartitionID int      `json:"partitionId"`
	Leader      string   `json:"leader"`
	Replicas    []string `json:"replicas"`
}
