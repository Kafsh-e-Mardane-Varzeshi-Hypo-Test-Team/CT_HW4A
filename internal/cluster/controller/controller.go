package controller

import (
	"errors"
	"log"
	"sync"
)

type Controller struct {
	mu                sync.Mutex
	partitionCount    int
	replicationFactor int
	Nodes             map[int]*NodeMetadata
	Partitions        []*PartitionMetadata
}

func NewController(partitionCount, replicationFactor int) *Controller {
	return &Controller{
		partitionCount:    partitionCount,
		replicationFactor: replicationFactor,
		Nodes:             make(map[int]*NodeMetadata),
		Partitions:        make([]*PartitionMetadata, partitionCount),
	}
}

func (c *Controller) RegisterNode(node *NodeMetadata) error {
	c.mu.Lock()
	if _, exists := c.Nodes[node.ID]; exists {
		c.mu.Unlock()
		log.Printf("controller::register-node: Node %d already exists.\n", node.ID)
		return errors.New("node already exists")
	}
	c.mu.Unlock()

	// TODO: Create a new docker container for the node

	// TODO: Find the partitions for the node

	// TODO: Request to create the partitions

	// TODO: Wait for the node to be ready

	// TODO: Update the metadata

	return nil
}

func (c *Controller) Start() {
}
