package controller

import (
	"errors"
	"log"
	"math/rand/v2"
	"strconv"
	"sync"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller/docker"
	"github.com/docker/go-connections/nat"
)

type Controller struct {
	dockerClient      *docker.DockerClient
	mu                sync.Mutex
	partitionCount    int
	replicationFactor int
	Nodes             map[int]*NodeMetadata
	Partitions        []*PartitionMetadata
}

func NewController(dockerClient *docker.DockerClient, partitionCount, replicationFactor int) *Controller {
	return &Controller{
		dockerClient:      dockerClient,
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
		log.Printf("controller::RegisterNode: Node %d already exists.\n", node.ID)
		return errors.New("node already exists")
	}
	c.mu.Unlock()

	// Create a new docker container for the node
	imageName := "helloworld:1" // TODO
	nodeName := "node-" + strconv.Itoa(node.ID)
	networkName := "temp" // TODO
	exposedPort := "8080/tcp"

	err := c.dockerClient.CreateNodeContainer(
		imageName,
		nodeName,
		networkName,
		nat.Port(exposedPort),
	)
	if err != nil {
		log.Printf("controller::RegisterNode: Failed to create docker container for node %d\n", node.ID)
		return errors.New("failed to create container")
	}

	c.mu.Lock()
	c.Nodes[node.ID] = &NodeMetadata{
		ID:      node.ID,
		Address: nodeName,
		Status:  Creating,
	}
	c.mu.Unlock()

	go c.makeNodeReady(node.ID)

	return nil
}

func (c *Controller) Start() {

}

func (c *Controller) makeNodeReady(nodeID int) {
	var partitions []int
	c.mu.Lock()
	for _, partition := range c.Partitions {
		if len(partition.Replicas) < c.replicationFactor {
			partitions = append(partitions, partition.PartitionID)
			c.Partitions[partition.PartitionID].Replicas = append(c.Partitions[partition.PartitionID].Replicas, nodeID)
		}
	}
	c.mu.Unlock()

	if len(partitions) == 0 {
		partitionID := rand.IntN(c.partitionCount)
		partitions = append(partitions, partitionID)
	}

	for _, partition := range partitions {
		c.replicate(partition, nodeID)
	}

	c.waitForNodeReady()

	c.mu.Lock()
	c.Nodes[nodeID].Status = Up
	c.mu.Unlock()
	log.Printf("controller::makeNodeReady: Node %d is now ready\n", nodeID)
}

func (c *Controller) replicate(partitionID, nodeID int) {
	// TODO: request to partition leader node to replicate in the new node
	// retry mechanism
	// remove nodeID from partition if replication fails

	log.Printf("controller::replicate: Partition %d replicated to node %d\n", partitionID, nodeID)
}

func (c *Controller) waitForNodeReady() error {
	/* timeout := time.Minute
	checkInterval := 5 * time.Second
	startTime := time.Now() */

	// TODO: request to the new node to check if it is ready
	// retry mechanism

	return errors.New("timeout waiting for node to become ready for partition")
}
