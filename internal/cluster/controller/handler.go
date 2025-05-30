package controller

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (c *Controller) setupRoutes() {
	c.ginEngine.GET("/metadata", c.handleGetMetadata)
	c.ginEngine.GET("/node-metadata/:partitionID", c.handleGetNodeMetadata)

	c.ginEngine.POST("/node-heartbeat", c.handleHeartbeat)

	c.ginEngine.POST("/nodes", c.handleRegisterNode)

	c.ginEngine.POST("/partitions/move-replica", c.handleMoveReplica)
	c.ginEngine.POST("/partitions/set-leader", c.handleSetLeader)
}

func (c *Controller) Run(addr string) error {
	return c.ginEngine.Run(addr)
}

func (c *Controller) handleGetMetadata(ctx *gin.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	metadata := struct {
		NodeAddresses map[int]string       `json:"nodes"`
		Partitions    []*PartitionMetadata `json:"partitions"`
	}{}
	metadata.NodeAddresses = make(map[int]string)
	for id, node := range c.nodes {
		if node.Status == Alive {
			metadata.NodeAddresses[id] = node.HttpAddress
		}
	}
	metadata.Partitions = c.partitions

	ctx.JSON(http.StatusOK, metadata)
}

func (c *Controller) handleGetNodeMetadata(ctx *gin.Context) {
	partitionID, err := strconv.Atoi(ctx.Param("partitionID"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	metadata := struct {
		Addresses []string `json:"addresses"`
	}{}
	c.mu.Lock()
	metadata.Addresses = make([]string, len(c.partitions[partitionID].Replicas))
	for i, replica := range c.partitions[partitionID].Replicas {
		if c.nodes[replica].Status == Alive {
			metadata.Addresses[i] = c.nodes[replica].TcpAddress
		}
	}
	c.mu.Unlock()

	ctx.JSON(http.StatusOK, metadata)
}

func (c *Controller) handleHeartbeat(ctx *gin.Context) {
	nodeID, err := strconv.Atoi(ctx.PostForm("NodeID"))
	if err != nil {
		return
	}

	c.mu.Lock()
	if c.nodes[nodeID].Status == Dead {
		log.Printf("controller::handleHeartbeat: Node %d revived\n", nodeID)
		go c.reviveNode(nodeID)
	}
	c.nodes[nodeID].lastSeen = time.Now()
	c.mu.Unlock()
	ctx.Status(http.StatusOK)
}

func (c *Controller) handleRegisterNode(ctx *gin.Context) {
	nodeID, err := strconv.Atoi(ctx.PostForm("NodeID"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid node ID"})
		return
	}

	err = c.RegisterNode(nodeID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register node"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Node is creating"})
}

func (c *Controller) handleRemoveNode(ctx *gin.Context) {

}

func (c *Controller) handleSetLeader(ctx *gin.Context) {
	var req struct {
		PartitionID int `json:"partition_id"`
		NodeID      int `json:"node_id"`
	}
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	c.mu.Lock()
	if req.PartitionID < 0 || req.PartitionID >= len(c.partitions) {
		c.mu.Unlock()
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}
	if req.NodeID < 0 || req.NodeID >= len(c.nodes) || c.nodes[req.NodeID].Status != Alive {
		c.mu.Unlock()
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid node ID"})
		return
	}

	if c.partitions[req.PartitionID].Leader == req.NodeID {
		c.mu.Unlock()
		ctx.JSON(http.StatusOK, gin.H{"message": "Node is already the leader"})
		return
	}

	// Check if the new leader is already a replica, if not error
	partition := c.partitions[req.PartitionID]
	exists := false
	for _, replica := range partition.Replicas {
		if replica == req.NodeID {
			exists = true
			break
		}
	}
	if !exists {
		c.mu.Unlock()
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Node is not a replica of the partition"})
		return
	}
	c.mu.Unlock()

	c.changeLeader(req.PartitionID, req.NodeID)
	
	ctx.JSON(http.StatusOK, gin.H{"message": "Leader set successfully"})
	log.Printf("controller::handleSetLeader: Node %d is now the leader for partition %d\n", req.NodeID, req.PartitionID)
}

func (c *Controller) handleRebalance(ctx *gin.Context) {

}

func (c *Controller) handleMoveReplica(ctx *gin.Context) {

}

func (c *Controller) handleReadyCheck(ctx *gin.Context) {

}
