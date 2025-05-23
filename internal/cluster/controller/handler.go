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
}

func (c *Controller) Run(addr string) error {
	if err := c.RegisterNode(1); err != nil {
		log.Fatalf("Failed to create initial node: %v", err)
	}
	return c.ginEngine.Run(addr)
}

func (c *Controller) handleGetMetadata(ctx *gin.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	metadata := struct {
		Partitions []*PartitionMetadata `json:"partitions"`
	}{}
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
		metadata.Addresses[i] = c.nodes[replica].Address
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
	c.nodes[nodeID].lastSeen = time.Now()
	c.mu.Unlock()
	ctx.Status(http.StatusOK)
}

func (c *Controller) handleRegisterNode(ctx *gin.Context) {

}

func (c *Controller) handleRemoveNode(ctx *gin.Context) {

}

func (c *Controller) handleSetLeader(ctx *gin.Context) {

}

func (c *Controller) handleRebalance(ctx *gin.Context) {

}

func (c *Controller) handleMoveReplica(ctx *gin.Context) {

}

func (c *Controller) handleReadyCheck(ctx *gin.Context) {

}
