package node

import (
	"log"
	"net/http"
	"strconv"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
	"github.com/gin-gonic/gin"
)

func (n *Node) setupRoutes() {
	n.ginEngine.POST("/add-partition/:partition-id", n.handleAddPartition)
	n.ginEngine.POST("/delete-partition/:partition-id", n.handleDeletePartition)
	// n.ginEngine.DELETE("/delete-partition", n.handleDeletePartition)
}

func (n *Node) handleAddPartition(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleAddPartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if _, ok := n.replicas[partitionId]; ok {
		log.Printf("[node.handleAddPartition] partitionId %v already exists in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusConflict, gin.H{"error": "this partitionId already exists"})
		return
	}

	n.replicas[partitionId] = replica.NewReplica(5, n.Id, partitionId, replica.Leader)
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleDeletePartition(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleDeletePartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if _, ok := n.replicas[partitionId]; !ok {
		log.Printf("[node.handleDeletePartition] partitionId %v does not exist in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusNotFound, gin.H{"error": "this partitionId does not exist"})
		return
	}

	n.replicas[partitionId] = nil
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleSendPartition() {
	// TODO
}
