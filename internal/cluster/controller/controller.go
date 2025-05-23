package controller

import (
	"errors"
	"log"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller/docker"
	"github.com/docker/go-connections/nat"
	"github.com/gin-gonic/gin"
)

type Controller struct {
	dockerClient      *docker.DockerClient
	ginEngine         *gin.Engine
	mu                sync.Mutex
	partitionCount    int
	replicationFactor int
	nodes             map[int]*NodeMetadata
	partitions        []*PartitionMetadata
	httpClient        *http.Client
	networkName       string
	nodeImage         string
}

func NewController(dockerClient *docker.DockerClient, partitionCount, replicationFactor int, networkName, nodeImage string) *Controller {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	c := &Controller{
		dockerClient:      dockerClient,
		ginEngine:         router,
		partitionCount:    partitionCount,
		replicationFactor: replicationFactor,
		nodes:             make(map[int]*NodeMetadata),
		partitions:        make([]*PartitionMetadata, partitionCount),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		},
		networkName: networkName,
		nodeImage:   nodeImage,
	}

	for i := range partitionCount {
		c.partitions[i] = &PartitionMetadata{
			PartitionID: i,
			Leader:      -1, // Will be set when first node joins
			Replicas:    make([]int, 0),
		}
	}

	c.setupRoutes()
	// go c.eventHandler()

	return c
}

func (c *Controller) RegisterNode(nodeID int) error {
	c.mu.Lock()
	if _, exists := c.nodes[nodeID]; exists {
		c.mu.Unlock()
		log.Printf("controller::RegisterNode: Node %d already exists.\n", nodeID)
		return errors.New("node already exists")
	}
	c.mu.Unlock()

	// Create a new docker container for the node
	imageName := c.nodeImage
	nodeName := "node-" + strconv.Itoa(nodeID)
	networkName := c.networkName
	exposedPort := "8080/tcp"

	err := c.dockerClient.CreateNodeContainer(
		imageName,
		nodeName,
		networkName,
		nat.Port(exposedPort),
	)
	if err != nil {
		log.Printf("controller::RegisterNode: Failed to create docker container for node %d\n", nodeID)
		return errors.New("failed to create container")
	}

	c.mu.Lock()
	c.nodes[nodeID] = &NodeMetadata{
		ID:      nodeID,
		Address: nodeName,
		Status:  Creating,
	}
	c.mu.Unlock()

	go c.makeNodeReady(nodeID)

	return nil
}

func (c *Controller) Start() {

}

func (c *Controller) makeNodeReady(nodeID int) {
	partitionsToAssign := make([]int, 0)
	c.mu.Lock()
	for _, partition := range c.partitions {
		if len(partition.Replicas) < c.replicationFactor {
			partitionsToAssign = append(partitionsToAssign, partition.PartitionID)
		}
	}
	c.mu.Unlock()

	if len(partitionsToAssign) == 0 {
		partitionID := rand.IntN(c.partitionCount)
		partitionsToAssign = append(partitionsToAssign, partitionID)
	}

	for _, partition := range partitionsToAssign {
		c.replicate(partition, nodeID)
	}

	// c.waitForNodeReady()

	c.mu.Lock()
	c.nodes[nodeID].Status = Alive
	c.mu.Unlock()
	log.Printf("controller::makeNodeReady: Node %d is now ready\n", nodeID)
}

func (c *Controller) replicate(partitionID, nodeID int) {
	c.mu.Lock()
	if c.partitions[partitionID].Leader == -1 {
		c.partitions[partitionID].Leader = nodeID
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	// TODO: request to partition leader node to replicate in the new node
	// retry mechanism
	// add nodeID to partition

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
