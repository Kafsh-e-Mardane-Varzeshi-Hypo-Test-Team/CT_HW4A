package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/controller/docker"
	"github.com/docker/go-connections/nat"
	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Controller struct {
	id                int
	dockerClient      *docker.DockerClient
	ginEngine         *gin.Engine
	partitionCount    int
	replicationFactor int

	etcdClient *clientv3.Client
	election   *concurrency.Election

	httpClient  *http.Client
	networkName string
	nodeImage   string
}

func NewController(id int, dockerClient *docker.DockerClient, networkName, nodeImage string, endpoints []string) *Controller {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("controller::NewController Failed to create etcd client: %v", err)
	}

	session, err := concurrency.NewSession(cli, concurrency.WithTTL(5))
	if err != nil {
		log.Fatalf("controller::NewController: failed to create etcd session, %v", err)
	}
	election := concurrency.NewElection(session, "controller/leader")

	// Read configuration from environment variables with sensible defaults
	partitionCount := 3 // default value
	if envPartitions := os.Getenv("PARTITION_COUNT"); envPartitions != "" {
		if parsed, err := strconv.Atoi(envPartitions); err == nil && parsed > 0 {
			partitionCount = parsed
		}
	}
	log.Printf("controller::NewController: partition count is %d", partitionCount)

	replicationFactor := 2 // default value
	if envReplication := os.Getenv("REPLICATION_FACTOR"); envReplication != "" {
		if parsed, err := strconv.Atoi(envReplication); err == nil && parsed > 0 {
			replicationFactor = parsed
		}
	}
	log.Printf("controller::NewController: replication factor is %d", replicationFactor)

	c := &Controller{
		id:                id,
		dockerClient:      dockerClient,
		ginEngine:         router,
		partitionCount:    partitionCount,
		replicationFactor: replicationFactor,

		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		},
		etcdClient:  cli,
		election:    election,
		networkName: networkName,
		nodeImage:   nodeImage,
	}

	c.setupRoutes()

	return c
}

func (c *Controller) RegisterNode(nodeID int) error {
	nodeMetadata := &NodeMetadata{
		ID:     nodeID,
		Status: Creating,
	}

	nodeJSON, err := json.Marshal(nodeMetadata)
	if err != nil {
		log.Printf("controller::RegisterNode: Failed to marshal node metadata: %v\n", err)
		return errors.New("failed to marshal node metadata")
	}

	ctx := context.Background()

	key := fmt.Sprintf("nodes/%d", nodeID)
	txnResp, err := c.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
		clientv3.Compare(clientv3.Version(key), "=", 0),
	).Then(
		clientv3.OpPut(key, string(nodeJSON)),
	).Commit()

	if err != nil {
		return fmt.Errorf("etcd transaction failed: %w", err)
	}

	if !txnResp.Succeeded {
		log.Printf("controller::RegisterNode: Not leader or Node %d already exists.\n", nodeID)
		return errors.New("not leader or node already exists")
	}

	// Create a new docker container for the node
	imageName := c.nodeImage
	networkName := c.networkName
	tcpPort := "9000"
	httpPort := "8000"

	err = c.dockerClient.CreateNodeContainer(
		imageName,
		nodeID,
		networkName,
		nat.Port(tcpPort),
		nat.Port(httpPort),
	)
	if err != nil {
		log.Printf("controller::RegisterNode: Failed to create docker container for node %d\n", nodeID)
		return errors.New("failed to create container")
	}

	nodeMetadata.TcpAddress = fmt.Sprintf("node-%d:%s", nodeID, tcpPort)
	nodeMetadata.HttpAddress = fmt.Sprintf("http://node-%d:%s", nodeID, httpPort)
	nodeMetadata.Status = Syncing

	nodeJSON, err = json.Marshal(nodeMetadata)
	if err != nil {
		log.Printf("controller::RegisterNode: Failed to marshal node metadata: %v\n", err)
		return errors.New("failed to marshal node metadata")
	}

	txnResp, err = c.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
	).Then(
		clientv3.OpPut(key, string(nodeJSON)),
	).Commit()

	if err != nil {
		return fmt.Errorf("etcd transaction failed: %w", err)
	}

	if !txnResp.Succeeded {
		log.Printf("controller::RegisterNode: Not leader")
		return errors.New("not leader")
	}

	go c.makeNodeReady(nodeMetadata)

	return nil
}

func (c *Controller) removeNode(nodeID int) error {
	ctx := context.Background()

	key := fmt.Sprintf("nodes/%d", nodeID)
	resp, err := c.etcdClient.Get(ctx, key)
	if err != nil || len(resp.Kvs) == 0 {
		return errors.New("node not found")
	}

	var node NodeMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &node); err != nil {
		log.Printf("Invalid node data: %v", err)
		return errors.New("failed to unmarshal node data")
	}

	txnResp, err := c.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
	).Then(
		clientv3.OpDelete(key),
	).Commit()

	if err != nil {
		return fmt.Errorf("etcd transaction failed: %w", err)
	}

	if !txnResp.Succeeded {
		log.Printf("controller::removeNode: Not leader")
		return errors.New("not leader")
	}

	err = c.dockerClient.RemoveNodeContainer(nodeID)
	if err != nil {
		log.Printf("controller::removeNode: Failed to remove docker container for node %d: %v\n", nodeID, err)
		return err
	}

	log.Printf("controller::removeNode: Node %d removed successfully\n", nodeID)

	return nil
}

func (c *Controller) Start(addr string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := c.election.Campaign(context.Background(), fmt.Sprintf("controller-%d", c.id))
	if err != nil {
		log.Fatalf("controller::Start: failed to campaign for leader election, %v", err)
	}
	log.Printf("I'm the leader, controller-%d", c.id)

	// Check if we need initialization by looking for existing nodes
	getResp, err := c.etcdClient.Get(context.Background(), "nodes/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		log.Printf("controller::Start: failed to check existing nodes: %v", err)
	} else if getResp.Count == 0 {
		log.Printf("controller::Start: No nodes exist, initializing partitions")
		c.initializePartitions()
	}

	go c.watchNodeStatus()

	go func() {
		if err := c.ginEngine.Run(addr); err != nil {
			log.Fatalf("controller::Start: Failed to run http server %v", err)
		}
	}()

	go func() {
		if err := c.RegisterNode(1); err != nil {
			log.Printf("controller::Start: Failed to create initial node: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("controller::Start: Shutting down gracefully...")
}

func (c *Controller) initializePartitions() {
	log.Printf("controller::initializePartitions: Initializing %d partitions", c.partitionCount)

	// Check if partitions already exist
	getResp, err := c.etcdClient.Get(context.Background(), "partitions/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		log.Printf("controller::initializePartitions: failed to check existing partitions: %v", err)
		return
	}

	if getResp.Count > 0 {
		log.Printf("controller::initializePartitions: Partitions already exist, skipping initialization")
		return
	}

	// Initialize partitions
	txnOps := make([]clientv3.Op, 0, c.partitionCount)
	for i := 0; i < c.partitionCount; i++ {
		partition := &PartitionMetadata{
			PartitionID: i,
			Leader:      -1,
			Replicas:    []int{},
		}
		data, _ := json.Marshal(partition)
		txnOps = append(txnOps,
			clientv3.OpPut(
				fmt.Sprintf("partitions/%d", i),
				string(data),
			))
	}

	txnResp, err := c.etcdClient.Txn(context.Background()).
		If(clientv3.Compare(
			clientv3.Version("partitions/0"),
			"=",
			0,
		)).
		Then(txnOps...).
		Commit()

	if err != nil {
		log.Printf("controller::initializePartitions: partition initialization failed: %v", err)
		return
	}

	if !txnResp.Succeeded {
		log.Printf("controller::initializePartitions: partitions already exist")
		return
	}

	log.Printf("controller::initializePartitions: Successfully initialized %d partitions", c.partitionCount)
}

func (c *Controller) makeNodeReady(node *NodeMetadata) {
	partitionsToAssign := make([]int, 0)
	resp, err := c.etcdClient.Get(context.Background(), "partitions/", clientv3.WithPrefix())
	if err != nil {
		log.Printf("controller::makeNodeReady: failed to get partitions to assign: %w", err)
		return
	}

	for _, kv := range resp.Kvs {
		var partition PartitionMetadata
		if err := json.Unmarshal(kv.Value, &partition); err != nil {
			continue
		}

		if len(partition.Replicas) < c.replicationFactor {
			partitionsToAssign = append(partitionsToAssign, partition.PartitionID)
		}
	}

	c.replicatePartitions(partitionsToAssign, node.ID)

	node.Status = Alive
	finalMeta, err := json.Marshal(node)
	if err != nil {
		log.Printf("failed to marshal final metadata: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	nodeKey := fmt.Sprintf("nodes/%d", node.ID)

	// Use transaction to ensure leadership and atomic update
	txnResp, err := c.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
	).Then(
		clientv3.OpPut(nodeKey, string(finalMeta)),
	).Commit()

	if err != nil {
		log.Printf("failed to mark node as alive: %v", err)
		return
	}

	if !txnResp.Succeeded {
		log.Printf("controller::makeNodeReady: Not leader, cannot mark node %d as alive", node.ID)
		return
	}

	log.Printf("controller::makeNodeReady: Node %d is now ready\n", node.ID)
}

func (c *Controller) replicatePartitions(partitionsToAssign []int, nodeID int) {
	for _, partitionID := range partitionsToAssign {
		for i := 0; i < 3; i++ {
			resp, err := c.etcdClient.Get(context.Background(),
				fmt.Sprintf("partitions/%d", partitionID))
			if err != nil {
				log.Printf("Failed to get partition %d: %v", partitionID, err)
				continue
			}

			var partition PartitionMetadata
			if len(resp.Kvs) > 0 {
				if err := json.Unmarshal(resp.Kvs[0].Value, &partition); err != nil {
					log.Printf("Bad partition data for %d: %v", partitionID, err)
					continue
				}
			} else {
				partition = PartitionMetadata{
					PartitionID: partitionID,
					Leader:      -1,
					Replicas:    []int{},
				}
			}

			log.Printf("controller::replicatePartitions: Attempting to assign partition %d to node %d", partitionID, nodeID)

			if err := c.replicate(partition, nodeID); err != nil {
				log.Printf("Replication attempt %d failed for partition %d: %v",
					i+1, partitionID, err)
				time.Sleep(time.Second * time.Duration(i+1))
				continue
			}

			txnResp, err := c.etcdClient.Txn(context.Background()).
				If(
					clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
					clientv3.Compare(
						clientv3.ModRevision(fmt.Sprintf("partitions/%d", partitionID)),
						"=",
						resp.Kvs[0].ModRevision,
					),
				).
				Then(
					clientv3.OpPut(
						fmt.Sprintf("partitions/%d", partitionID),
						c.updatePartitionData(partition, nodeID),
					),
					// Update node's partition list
					clientv3.OpPut(
						fmt.Sprintf("nodes/%d/partitions", nodeID),
						c.updateNodePartitions(nodeID, partitionID),
					),
				).
				Commit()

			if err != nil {
				log.Printf("Transaction failed for partition %d: %v", partitionID, err)
				continue
			}

			if !txnResp.Succeeded {
				log.Printf("Partition %d modified by another controller or not leader, retrying", partitionID)
				continue
			}

			log.Printf("Successfully assigned partition %d to node %d", partitionID, nodeID)
			break // Move to next partition
		}
	}
}

func (c *Controller) updatePartitionData(p PartitionMetadata, nodeID int) string {
	if p.Leader == -1 {
		p.Leader = nodeID
	} else {
		p.Replicas = append(p.Replicas, nodeID)
	}
	data, _ := json.Marshal(p)
	return string(data)
}

func (c *Controller) updateNodePartitions(nodeID, partitionID int) string {
	resp, _ := c.etcdClient.Get(context.Background(),
		fmt.Sprintf("nodes/%d/partitions", nodeID))

	var partitions []int
	if len(resp.Kvs) > 0 {
		json.Unmarshal(resp.Kvs[0].Value, &partitions)
	}
	partitions = append(partitions, partitionID)
	data, _ := json.Marshal(partitions)
	return string(data)
}

func (c *Controller) replicate(partition PartitionMetadata, nodeID int) error {
	// Get node metadata from etcd
	nodeKey := fmt.Sprintf("nodes/%d", nodeID)
	nodeResp, err := c.etcdClient.Get(context.Background(), nodeKey)
	if err != nil || len(nodeResp.Kvs) == 0 {
		return fmt.Errorf("failed to get node %d metadata: %w", nodeID, err)
	}

	var node NodeMetadata
	if err := json.Unmarshal(nodeResp.Kvs[0].Value, &node); err != nil {
		return fmt.Errorf("failed to unmarshal node %d metadata: %w", nodeID, err)
	}

	var addr string
	if partition.Leader == -1 {
		addr = fmt.Sprintf("%s/add-partition/%d", node.HttpAddress, partition.PartitionID)
	} else {
		// Get leader node metadata from etcd
		leaderKey := fmt.Sprintf("nodes/%d", partition.Leader)
		leaderResp, err := c.etcdClient.Get(context.Background(), leaderKey)
		if err != nil || len(leaderResp.Kvs) == 0 {
			return fmt.Errorf("failed to get leader node %d metadata: %w", partition.Leader, err)
		}

		var leaderNode NodeMetadata
		if err := json.Unmarshal(leaderResp.Kvs[0].Value, &leaderNode); err != nil {
			return fmt.Errorf("failed to unmarshal leader node %d metadata: %w", partition.Leader, err)
		}

		addr = fmt.Sprintf("%s/send-partition/%d/%s", leaderNode.HttpAddress, partition.PartitionID, node.TcpAddress)
	}

	httpResp, err := c.doNodeRequest("POST", addr)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		return errors.New("resp status not OK: " + httpResp.Status)
	}

	return nil
}

func (c *Controller) doNodeRequest(method, addr string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, addr, nil)
	if err != nil {
		return nil, err
	}

	return c.httpClient.Do(req)
}

func (c *Controller) handleFailover(nodeID int) {
	log.Printf("Starting failover for node %d", nodeID)

	// Verify leadership before handling failover
	ctx := context.Background()
	txnResp, err := c.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
	).Then(
		clientv3.OpGet("dummy_key"),
	).Commit()

	if err != nil {
		log.Printf("controller::handleFailover: Leadership verification failed: %v", err)
		return
	}

	if !txnResp.Succeeded {
		log.Printf("controller::handleFailover: Not leader, ignoring failover for node %d", nodeID)
		return
	}

	nodeKey := fmt.Sprintf("nodes/%d", nodeID)
	resp, err := c.etcdClient.Get(ctx, nodeKey)
	if err != nil {
		log.Printf("failed to check node status: %v", err)
		return
	}

	if len(resp.Kvs) == 0 {
		log.Printf("node does not exist")
		return
	}

	var node NodeMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &node); err != nil {
		log.Printf("invalid node data: %v", err)
		return
	}

	if node.Status != Dead {
		return
	}

	partitionsKey := fmt.Sprintf("nodes/%d/partitions", nodeID)
	resp, err = c.etcdClient.Get(ctx, partitionsKey)
	if err != nil {
		log.Printf("failed to get node partitions: %v", err)
		return
	}

	var partitions []int
	if len(resp.Kvs) > 0 {
		if err := json.Unmarshal(resp.Kvs[0].Value, &partitions); err != nil {
			log.Printf("failed to unmarshal node partitions: %v", err)
			return
		}
	}

	for _, partitionID := range partitions {
		if err := c.handlePartitionFailover(partitionID, nodeID); err != nil {
			log.Printf("Failed to handle partition %d: %v", partitionID, err)
			continue
		}
	}
}

func (c *Controller) handlePartitionFailover(partitionID, deadNodeID int) error {
	partitionKey := fmt.Sprintf("partitions/%d", partitionID)

	for attempt := 0; attempt < 3; attempt++ {
		resp, err := c.etcdClient.Get(context.Background(), partitionKey)
		if err != nil {
			return fmt.Errorf("failed to get partition %d: %w", partitionID, err)
		}

		if len(resp.Kvs) == 0 {
			return fmt.Errorf("partition %d not found", partitionID)
		}

		var partition PartitionMetadata
		if err := json.Unmarshal(resp.Kvs[0].Value, &partition); err != nil {
			return fmt.Errorf("invalid partition data: %w", err)
		}

		var ops []clientv3.Op
		var newLeader int = -1

		if partition.Leader == deadNodeID {
			if len(partition.Replicas) == 0 {
				return fmt.Errorf("no available replicas for partition %d", partitionID)
			}

			newLeader = partition.Replicas[0]
			partition.Leader = newLeader
			partition.Replicas = partition.Replicas[1:]
			ops = append(ops, clientv3.OpPut(partitionKey, partition.ToJson()))

			go c.notifyNewLeader(newLeader, partitionID)
		} else {
			newReplicas := make([]int, 0, len(partition.Replicas)-1)
			for _, r := range partition.Replicas {
				if r != deadNodeID {
					newReplicas = append(newReplicas, r)
				}
			}
			partition.Replicas = newReplicas
			ops = append(ops, clientv3.OpPut(partitionKey, partition.ToJson()))
		}

		txnResp, err := c.etcdClient.Txn(context.Background()).
			If(
				clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
				clientv3.Compare(
					clientv3.ModRevision(partitionKey),
					"=",
					resp.Kvs[0].ModRevision,
				),
			).
			Then(ops...).
			Commit()

		if err != nil {
			return fmt.Errorf("transaction failed: %w", err)
		}

		if txnResp.Succeeded {
			log.Printf("Failover completed for partition %d", partitionID)
			return nil
		}

		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}

	return fmt.Errorf("max retries reached for partition %d", partitionID)
}

func (c *Controller) notifyNewLeader(nodeID, partitionID int) {
	resp, err := c.etcdClient.Get(context.Background(),
		fmt.Sprintf("nodes/%d", nodeID))
	if err != nil || len(resp.Kvs) == 0 {
		log.Printf("Failed to get node %d info: %v", nodeID, err)
		return
	}

	var node NodeMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &node); err != nil {
		log.Printf("Invalid node data: %v", err)
		return
	}

	url := fmt.Sprintf("%s/set-leader/%d", node.HttpAddress, partitionID)
	httpResp, err := c.doNodeRequest("POST", url)
	if err != nil {
		log.Printf("Failed to notify new leader: %v", err)
		return
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		log.Printf("New leader returned status: %s", httpResp.Status)
	}
}

func (c *Controller) reviveNode(nodeID int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txnResp, err := c.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
	).Then(
		clientv3.OpGet("dummy_key"),
	).Commit()

	if err != nil {
		log.Printf("controller::reviveNode: Leadership verification failed: %v", err)
		return
	}

	if !txnResp.Succeeded {
		log.Printf("controller::reviveNode: Not leader, cannot revive node %d", nodeID)
		return
	}

	nodeKey := fmt.Sprintf("nodes/%d", nodeID)
	partitionsKey := fmt.Sprintf("nodes/%d/partitions", nodeID)

	getResp, err := c.etcdClient.Get(ctx, nodeKey)
	if err != nil {
		log.Printf("failed to get node metadata: %v", err)
		return
	}

	if len(getResp.Kvs) == 0 {
		log.Printf("node %d not found", nodeID)
		return
	}

	var node NodeMetadata
	if err := json.Unmarshal(getResp.Kvs[0].Value, &node); err != nil {
		log.Printf("failed to unmarshal node metadata: %v", err)
		return
	}

	if node.Status != Dead {
		return
	}

	log.Printf("controller::reviveNode: Reviving node %d\n", nodeID)

	getPartsResp, err := c.etcdClient.Get(ctx, partitionsKey)
	if err != nil {
		log.Printf("failed to get partitions: %v", err)
		return
	}

	var partitionsToReplicate []int
	if len(getPartsResp.Kvs) > 0 {
		if err := json.Unmarshal(getPartsResp.Kvs[0].Value, &partitionsToReplicate); err != nil {
			log.Printf("failed to unmarshal partitions: %v", err)
			return
		}
	}

	node.Status = Syncing
	updatedMeta, err := json.Marshal(node)
	if err != nil {
		log.Printf("failed to marshal updated metadata: %v", err)
		return
	}

	txnResp, err = c.etcdClient.Txn(ctx).
		If(
			clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
			clientv3.Compare(clientv3.Version(nodeKey), ">", 0),
		).
		Then(
			clientv3.OpPut(nodeKey, string(updatedMeta)),
		).
		Commit()

	if err != nil {
		log.Printf("failed to update node status: %v", err)
		return
	}
	if !txnResp.Succeeded {
		log.Printf("controller::reviveNode: Not leader or node metadata changed during revival")
		return
	}

	c.replicatePartitions(partitionsToReplicate, nodeID)

	node.Status = Alive
	finalMeta, err := json.Marshal(node)
	if err != nil {
		log.Printf("failed to marshal final metadata: %v", err)
		return
	}

	// Use transaction to ensure leadership and atomic update
	txnResp, err = c.etcdClient.Txn(ctx).
		If(
			clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
		).
		Then(
			clientv3.OpPut(nodeKey, string(finalMeta)),
		).
		Commit()

	if err != nil {
		log.Printf("failed to mark node as alive: %v", err)
		return
	}

	if !txnResp.Succeeded {
		log.Printf("controller::reviveNode: Not leader, cannot mark node %d as alive", nodeID)
		return
	}

	log.Printf("controller::reviveNode: Node %d revived successfully", nodeID)
}

func (c *Controller) changeLeader(partitionID, newLeaderID int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	txnResp, err := c.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
	).Then(
		clientv3.OpGet("dummy_key"),
	).Commit()

	if err != nil {
		return fmt.Errorf("leadership verification failed: %v", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("not leader, cannot change leader for partition %d", partitionID)
	}

	partitionKey := fmt.Sprintf("partitions/%d", partitionID)

	getResp, err := c.etcdClient.Get(ctx, partitionKey)
	if err != nil {
		return fmt.Errorf("failed to get partition metadata: %v", err)
	}
	if len(getResp.Kvs) == 0 {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	var partition PartitionMetadata
	if err := json.Unmarshal(getResp.Kvs[0].Value, &partition); err != nil {
		return fmt.Errorf("failed to unmarshal partition metadata: %v", err)
	}

	oldLeaderID := partition.Leader

	if oldLeaderID != -1 {
		nodeKey := fmt.Sprintf("nodes/%d", oldLeaderID)
		nodeResp, err := c.etcdClient.Get(ctx, nodeKey)
		if err != nil {
			return fmt.Errorf("failed to get old leader metadata: %v", err)
		}
		if len(nodeResp.Kvs) == 0 {
			return fmt.Errorf("old leader node %d not found", oldLeaderID)
		}

		var oldLeader NodeMetadata
		if err := json.Unmarshal(nodeResp.Kvs[0].Value, &oldLeader); err != nil {
			return fmt.Errorf("failed to unmarshal old leader metadata: %v", err)
		}

		addr := fmt.Sprintf("%s/set-follower/%d", oldLeader.HttpAddress, partitionID)
		resp, err := c.doNodeRequest("POST", addr)
		if err != nil {
			log.Printf("controller::changeLeader: Failed to notify old leader %d for partition %d: %v\n", oldLeaderID, partitionID, err)
			return fmt.Errorf("failed to notify old leader")
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("controller::changeLeader: Failed to set old leader %d as follower for partition %d: %s\n", oldLeaderID, partitionID, resp.Status)
			return fmt.Errorf("failed to set old leader as follower for partition %d: %v", partitionID, resp.Status)
		}
		log.Printf("controller::changeLeader: Node %d is now a follower for partition %d\n", oldLeaderID, partitionID)
	}

	c.notifyNewLeader(newLeaderID, partitionID)

	newReplicas := make([]int, 0, len(partition.Replicas))
	for _, replica := range partition.Replicas {
		if replica != newLeaderID {
			newReplicas = append(newReplicas, replica)
		}
	}
	if oldLeaderID != -1 && oldLeaderID != newLeaderID {
		newReplicas = append(newReplicas, oldLeaderID)
	}

	partition.Leader = newLeaderID
	partition.Replicas = newReplicas
	updatedPartition, err := json.Marshal(partition)
	if err != nil {
		return fmt.Errorf("failed to marshal updated partition metadata: %v", err)
	}

	txnResp, err = c.etcdClient.Txn(ctx).
		If(
			clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
			clientv3.Compare(
				clientv3.ModRevision(partitionKey),
				"=",
				getResp.Kvs[0].ModRevision,
			),
		).
		Then(
			clientv3.OpPut(partitionKey, string(updatedPartition)),
		).
		Commit()

	if err != nil {
		return fmt.Errorf("failed to update partition metadata: %v", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("not leader or partition %d was modified concurrently", partitionID)
	}

	log.Printf("controller::changeLeader: Partition %d leader changed to node %d\n", partitionID, newLeaderID)
	return nil
}

func (c *Controller) removePartitionReplica(partitionID, nodeID int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	txnResp, err := c.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
	).Then(
		clientv3.OpGet("dummy_key"),
	).Commit()

	if err != nil {
		return fmt.Errorf("leadership verification failed: %v", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("not leader, cannot remove partition replica")
	}

	partitionKey := fmt.Sprintf("partitions/%d", partitionID)

	getResp, err := c.etcdClient.Get(ctx, partitionKey)
	if err != nil {
		return fmt.Errorf("failed to get partition metadata: %v", err)
	}
	if len(getResp.Kvs) == 0 {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	var partition PartitionMetadata
	if err := json.Unmarshal(getResp.Kvs[0].Value, &partition); err != nil {
		return fmt.Errorf("failed to unmarshal partition metadata: %v", err)
	}

	replicasUpdated := false
	newReplicas := make([]int, 0, len(partition.Replicas))
	for _, replica := range partition.Replicas {
		if replica != nodeID {
			newReplicas = append(newReplicas, replica)
		} else {
			replicasUpdated = true
		}
	}

	if replicasUpdated {
		partition.Replicas = newReplicas
		updatedPartition, err := json.Marshal(partition)
		if err != nil {
			return fmt.Errorf("failed to marshal updated partition metadata: %v", err)
		}

		txnResp, err = c.etcdClient.Txn(ctx).
			If(
				clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
				clientv3.Compare(
					clientv3.ModRevision(partitionKey),
					"=",
					getResp.Kvs[0].ModRevision,
				),
			).
			Then(
				clientv3.OpPut(partitionKey, string(updatedPartition)),
			).
			Commit()

		if err != nil {
			return fmt.Errorf("failed to update partition metadata: %v", err)
		}

		if !txnResp.Succeeded {
			return fmt.Errorf("not leader or partition %d was modified concurrently", partitionID)
		}

		log.Printf("controller::removePartitionReplica: Removed node %d from replicas of partition %d\n", nodeID, partitionID)
		return nil
	}

	nodeKey := fmt.Sprintf("nodes/%d", nodeID)
	nodeResp, err := c.etcdClient.Get(ctx, nodeKey)
	if err != nil {
		return fmt.Errorf("failed to get node metadata: %v", err)
	}
	if len(nodeResp.Kvs) == 0 {
		return fmt.Errorf("node %d not found", nodeID)
	}

	var node NodeMetadata
	if err := json.Unmarshal(nodeResp.Kvs[0].Value, &node); err != nil {
		return fmt.Errorf("failed to unmarshal node metadata: %v", err)
	}

	addr := fmt.Sprintf("%s/delete-partition/%d", node.HttpAddress, partitionID)
	resp, err := c.doNodeRequest("POST", addr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("resp status not OK: " + resp.Status)
	}

	log.Printf("controller::removePartitionReplica: Node %d removed from partition %d successfully\n", nodeID, partitionID)
	return nil
}

func (c *Controller) watchNodeStatus() {
	ch := c.etcdClient.Watch(context.Background(), "nodes/active/", clientv3.WithPrefix())
	for resp := range ch {
		for _, event := range resp.Events {
			nodeID, err := strconv.Atoi(string(event.Kv.Key)[len("nodes/active/"):])
			if err != nil {
				log.Printf("controller::Start: Failed to parse node ID: %v", err)
				continue
			}

			resp, err := c.etcdClient.Get(context.Background(),
				fmt.Sprintf("nodes/%d", nodeID))
			if err != nil || len(resp.Kvs) == 0 {
				log.Printf("Failed to get node %d info: %v", nodeID, err)
				continue
			}

			var node NodeMetadata
			if err := json.Unmarshal(resp.Kvs[0].Value, &node); err != nil {
				log.Printf("Invalid node data: %v", err)
				continue
			}

			if event.Type == clientv3.EventTypeDelete {
				nodeKey := fmt.Sprintf("nodes/%d", nodeID)

				if node.Status == Alive {
					log.Printf("controller::monitorHeartbeat: Node %d is not responding\n", node.ID)

					node.Status = Dead
					updatedNode, err := json.Marshal(node)
					if err != nil {
						log.Printf("controller::monitorHeartbeat: Failed to marshal node %d data: %v\n", node.ID, err)
						continue
					}

					// Use transaction to ensure leadership and atomic update
					txnResp, err := c.etcdClient.Txn(context.Background()).
						If(
							clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
						).
						Then(
							clientv3.OpPut(nodeKey, string(updatedNode)),
						).
						Commit()

					if err != nil {
						log.Printf("controller::monitorHeartbeat: Failed to update node %d status in etcd: %v\n", node.ID, err)
						continue
					}

					if !txnResp.Succeeded {
						log.Printf("controller::monitorHeartbeat: Not leader, cannot mark node %d as dead", node.ID)
						continue
					}

					go c.handleFailover(node.ID)
				}
			} else if event.Type == clientv3.EventTypePut {
				if node.Status == Dead {
					log.Printf("controller::monitorHeartbeat: Node %d is back online", node.ID)
					txnResp, err := c.etcdClient.Txn(context.Background()).If(
						clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
					).Then(
						clientv3.OpGet("dummy_key"),
					).Commit()

					if err != nil {
						log.Printf("controller::Start: Leadership verification failed for node %d revival: %v", nodeID, err)
						continue
					}

					if txnResp.Succeeded {
						log.Printf("controller::Start: Leader processing revival for node %d", nodeID)
						go c.reviveNode(nodeID)
					} else {
						log.Printf("controller::Start: Not leader anymore, ignoring node %d revival", nodeID)
					}
				}
			}
		}
	}
}
