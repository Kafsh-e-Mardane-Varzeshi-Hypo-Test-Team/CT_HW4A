package controller

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

func (c *Controller) setupRoutes() {
	c.ginEngine.POST("/node/add", c.handleRegisterNode)
	c.ginEngine.POST("/node/remove", c.handleRemoveNode)

	c.ginEngine.POST("/partition/move-replica", c.handleMoveReplica)
	c.ginEngine.POST("/partition/set-leader", c.handleSetLeader)
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
	nodeID, err := strconv.Atoi(ctx.PostForm("NodeID"))
	nodeKey := fmt.Sprintf("node-%d", nodeID)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid node ID"})
		return
	}

	// Check if node exists using etcd
	resp, err := c.etcdClient.Get(ctx, nodeKey)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check node status"})
		return
	}
	if len(resp.Kvs) == 0 {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "Node not found"})
		return
	}

	// Mark node as Dead in etcd
	var node NodeMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &node); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse node data"})
		return
	}
	node.Status = Dead
	updatedNode, _ := json.Marshal(node)
	_, err = c.etcdClient.Put(ctx, nodeKey, string(updatedNode))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update node status"})
		return
	}

	c.handleFailover(nodeID)

	err = c.removeNode(nodeID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to remove node"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Node removed successfully"})
	log.Printf("controller::handleRemoveNode: Node %d removed\n", nodeID)
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

	partitionKey := fmt.Sprintf("partition-%d", req.PartitionID)

	// Get partition from etcd
	resp, err := c.etcdClient.Get(ctx, partitionKey)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get partition"})
		return
	}
	if len(resp.Kvs) == 0 {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	var partition PartitionMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &partition); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse partition data"})
		return
	}

	// Check if already leader
	if partition.Leader == req.NodeID {
		ctx.JSON(http.StatusOK, gin.H{"message": "Node is already the leader"})
		return
	}

	// Check if node is a replica
	exists := false
	for _, replica := range partition.Replicas {
		if replica == req.NodeID {
			exists = true
			break
		}
	}
	if !exists {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Node is not a replica of the partition"})
		return
	}

	err = c.changeLeader(req.PartitionID, req.NodeID)
	if err != nil {
		log.Printf("controller::handleSetLeader: Failed to set leader for partition %d: %v\n", req.PartitionID, err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to set leader"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Leader set successfully"})
	log.Printf("controller::handleSetLeader: Node %d is now the leader for partition %d\n", req.NodeID, req.PartitionID)
}

func (c *Controller) handleRebalance(ctx *gin.Context) {

}

func (c *Controller) handleMoveReplica(ctx *gin.Context) {
	var req struct {
		PartitionID int `json:"partition_id"`
		From        int `json:"from"`
		To          int `json:"to"`
	}
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	partitionKey := fmt.Sprintf("partition-%d", req.PartitionID)

	resp, err := c.etcdClient.Get(ctx, partitionKey)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get partition"})
		return
	}
	if len(resp.Kvs) == 0 {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	var partition PartitionMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &partition); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse partition data"})
		return
	}

	isLeader := partition.Leader == req.From
	if !isLeader {
		exists := false
		for _, replica := range partition.Replicas {
			if replica == req.From {
				exists = true
				break
			}
		}
		if !exists {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Replica not found in partition"})
			return
		}
	}

	log.Printf("controller::handleMoveReplica: Moving replica from node %d to node %d for partition %d\n", req.From, req.To, req.PartitionID)
	c.replicate(partition, req.To)
	if isLeader {
		err := c.changeLeader(req.PartitionID, req.To)
		if err != nil {
			log.Printf("controller::handleMoveReplica: Failed to set leader for partition %d: %v\n", req.PartitionID, err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to set leader"})
			return
		}
		log.Printf("controller::handleMoveReplica: Node %d is now the leader for partition %d after moving replica\n", req.To, req.PartitionID)
	}
	c.removePartitionReplica(req.PartitionID, req.From)

	log.Printf("controller::handleMoveReplica: Replica moved from node %d to node %d for partition %d\n", req.From, req.To, req.PartitionID)
	ctx.JSON(http.StatusOK, gin.H{"message": "Replica moved successfully"})
}

func (c *Controller) handleReadyCheck(ctx *gin.Context) {

}
