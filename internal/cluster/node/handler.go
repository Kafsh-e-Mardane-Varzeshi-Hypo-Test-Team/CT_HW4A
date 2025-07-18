package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/controller"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/replica"
	"github.com/gin-gonic/gin"
)

func (n *Node) setupRoutes() {
	// controller routes
	n.ginEngine.POST("/add-partition/:partition-id", n.handleAddPartition)
	n.ginEngine.POST("/set-leader/:partition-id", n.handleSetLeader)
	n.ginEngine.POST("/set-follower/:partition-id", n.handleSetFollower)
	n.ginEngine.DELETE("/delete-partition/:partition-id", n.handleDeletePartition)
	n.ginEngine.POST("/send-partition/:partition-id/:address", n.handleSendPartitionToNode)

	// loadbalancer routes
	n.ginEngine.POST("/:partition-id/:key/:value", n.handleSetRequest)
	n.ginEngine.GET("/:partition-id/:key", n.handleGetRequest)
	n.ginEngine.DELETE("/:partition-id/:key", n.handleDeleteRequest)
}

func (n *Node) handleAddPartition(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleAddPartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	n.replicasMapMutex.Lock()
	if _, ok := n.replicas[partitionId]; ok {
		log.Printf("[node.handleAddPartition] partitionId %v already exists in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusConflict, gin.H{"error": "this partitionId already exists"})
		return
	}

	n.replicas[partitionId] = replica.NewReplica(n.Id, partitionId, replica.Leader)
	n.replicasMapMutex.Unlock()
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleSetLeader(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleAddPartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	n.replicasMapMutex.Lock()
	if _, ok := n.replicas[partitionId]; !ok {
		n.replicasMapMutex.Unlock()
		log.Printf("[node.handleAddPartition] partitionId %v does not exist in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusConflict, gin.H{"error": "this partitionId does not exist"})
		return
	}

	n.replicas[partitionId].ConvertToLeader()
	n.replicasMapMutex.Unlock()
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleSetFollower(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleAddPartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	n.replicasMapMutex.Lock()
	if _, ok := n.replicas[partitionId]; !ok {
		n.replicasMapMutex.Unlock()
		log.Printf("[node.handleAddPartition] partitionId %v does not exist in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusConflict, gin.H{"error": "this partitionId does not exist"})
		return
	}

	n.replicas[partitionId].ConvertToFollower()
	n.replicasMapMutex.Unlock()
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleDeletePartition(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleDeletePartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	n.replicasMapMutex.Lock()
	if _, ok := n.replicas[partitionId]; !ok {
		log.Printf("[node.handleDeletePartition] partitionId %v does not exist in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusNotFound, gin.H{"error": "this partitionId does not exist"})
		return
	}

	delete(n.replicas, partitionId)
	n.replicasMapMutex.Unlock()
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleSendPartitionToNode(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleSendPartitionToNode] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	address := c.Param("address")
	if address == "" {
		log.Printf("[node.handleSendPartitionToNode] missing address parameter")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing address parameter"})
		return
	}
	log.Printf("[node.handleSendPartitionToNode] sending partition %d to address %s", partitionId, address)

	if err := n.sendSnapshotToNode(partitionId, address); err != nil {
		log.Printf("[node.handleSendPartitionToNode] %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Snapshot sent successfully"})

}

func (n *Node) handleSetRequest(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleSetRequest] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	key := c.Param("key")
	value := c.Param("value")

	err = n.set(partitionId, -1, key, value, replica.Leader)
	if err != nil {
		log.Printf("[node.handleSetRequest] failed to set key '%s' in partition %d: %v", key, partitionId, err)
		c.JSON(http.StatusNotAcceptable, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleGetRequest(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleGetRequest] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	key := c.Param("key")

	value, err := n.get(partitionId, key)
	if err != nil {
		log.Printf("[node.handleGetRequest] failed to get key '%s' from partition %d: %v", key, partitionId, err)
		c.JSON(http.StatusNotAcceptable, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"value": value})
}

func (n *Node) handleDeleteRequest(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleDeleteRequest] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	key := c.Param("key")

	err = n.delete(partitionId, -1, key, replica.Leader)
	if err != nil {
		log.Printf("[node.handleDeleteRequest] failed to delete key '%s' from partition %d: %v", key, partitionId, err)
		c.JSON(http.StatusNotAcceptable, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, nil)
}

func (n *Node) getNodesContainingPartition(partitionId int) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), REQUEST_TIMEOUT)
	defer cancel()

	// Get partition metadata from etcd
	partitionKey := fmt.Sprintf("partitions/%d", partitionId)
	partitionResp, err := n.etcdClient.Get(ctx, partitionKey)
	if err != nil {
		log.Printf("[node.getNodesContainingPartition] failed to get partition %d from etcd: %v", partitionId, err)
		return nil, err
	}

	if len(partitionResp.Kvs) == 0 {
		log.Printf("[node.getNodesContainingPartition] partition %d not found in etcd", partitionId)
		return nil, fmt.Errorf("partition %d not found", partitionId)
	}

	var partition controller.PartitionMetadata
	if err := json.Unmarshal(partitionResp.Kvs[0].Value, &partition); err != nil {
		log.Printf("[node.getNodesContainingPartition] failed to unmarshal partition data: %v", err)
		return nil, err
	}

	nodeIDs := make([]int, 0)
	nodeIDs = append(nodeIDs, partition.Replicas...)

	// Get node addresses from etcd
	var addresses []string
	for _, nodeID := range nodeIDs {
		nodeKey := fmt.Sprintf("nodes/%d", nodeID)
		nodeResp, err := n.etcdClient.Get(ctx, nodeKey)
		if err != nil {
			log.Printf("[node.getNodesContainingPartition] failed to get node %d from etcd: %v", nodeID, err)
			continue
		}

		if len(nodeResp.Kvs) == 0 {
			log.Printf("[node.getNodesContainingPartition] node %d not found in etcd", nodeID)
			continue
		}

		var node controller.NodeMetadata
		if err := json.Unmarshal(nodeResp.Kvs[0].Value, &node); err != nil {
			log.Printf("[node.getNodesContainingPartition] failed to unmarshal node %d data: %v", nodeID, err)
			continue
		}

		if node.Status == controller.Alive {
			tcpAddress := node.TcpAddress
			if tcpAddress == "" {
				tcpAddress = fmt.Sprintf("node-%d:9000", nodeID)
			}
			addresses = append(addresses, tcpAddress)
		}
	}

	log.Printf("[node.getNodesContainingPartition] found %d addresses for partition %d: %v", len(addresses), partitionId, addresses)
	return addresses, nil
}
