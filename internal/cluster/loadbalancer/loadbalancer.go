package loadbalancer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/controller"
	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type LoadBalancer struct {
	etcdClient       *clientv3.Client
	httpClient       *http.Client
	ginEngine        *gin.Engine
	metadataLock     sync.RWMutex
	metadataExpiry   time.Time
	refreshInterval  time.Duration
	requestTimeout   time.Duration
	maxRetries       int
	partitionCache   []*controller.PartitionMetadata
	nodeAddressCache map[int]string
}

func NewLoadBalancer(etcdEndpoints []string) *LoadBalancer {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	// Create etcd client
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}

	lb := &LoadBalancer{
		etcdClient: etcdClient,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		},
		ginEngine:       router,
		refreshInterval: 10 * time.Second,
		requestTimeout:  2 * time.Second,
		maxRetries:      3,
	}

	lb.setupRoutes()

	if err := lb.refreshMetadata(); err != nil {
		log.Printf("Initial metadata refresh failed: %v", err)
	}

	go lb.metadataRefresher()

	return lb
}

func (lb *LoadBalancer) getPartitionID(key string) int {
	h := sha256.Sum256([]byte(key))
	return int(h[0]) % len(lb.partitionCache)
}

func (lb *LoadBalancer) getLeaderForPartition(partitionID int) (string, error) {
	lb.metadataLock.RLock()
	defer lb.metadataLock.RUnlock()

	if len(lb.partitionCache) == 0 || time.Now().After(lb.metadataExpiry) {
		return "", errors.New("metadata not available or expired")
	}

	if partitionID >= len(lb.partitionCache) {
		return "", fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	return lb.nodeAddressCache[lb.partitionCache[partitionID].Leader], nil
}

func (lb *LoadBalancer) getReplicaForRead(partitionID int) (string, error) {
	lb.metadataLock.RLock()
	defer lb.metadataLock.RUnlock()

	if len(lb.partitionCache) == 0 || time.Now().After(lb.metadataExpiry) {
		return "", errors.New("metadata not available or expired")
	}

	if partitionID >= len(lb.partitionCache) {
		return "", fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	replicas := lb.partitionCache[partitionID].Replicas
	if len(replicas) == 0 {
		return lb.nodeAddressCache[lb.partitionCache[partitionID].Leader], nil
	}

	return lb.nodeAddressCache[replicas[rand.Intn(len(replicas))]], nil
}

func (lb *LoadBalancer) refreshMetadata() error {
	ctx, cancel := context.WithTimeout(context.Background(), lb.requestTimeout)
	defer cancel()

	// Fetch partitions from etcd
	partitionsResp, err := lb.etcdClient.Get(ctx, "partitions/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to fetch partitions from etcd: %w", err)
	}

	var partitions []*controller.PartitionMetadata
	for _, kv := range partitionsResp.Kvs {
		var partition controller.PartitionMetadata
		if err := json.Unmarshal(kv.Value, &partition); err != nil {
			log.Printf("Failed to unmarshal partition data: %v", err)
			continue
		}
		partitions = append(partitions, &partition)
	}

	// Fetch nodes from etcd
	nodesResp, err := lb.etcdClient.Get(ctx, "nodes/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to fetch nodes from etcd: %w", err)
	}

	nodeAddresses := make(map[int]string)
	for _, kv := range nodesResp.Kvs {
		// Skip partition lists and other non-node metadata
		keyStr := string(kv.Key)
		if keyStr == "nodes/" || len(keyStr) <= len("nodes/") {
			continue
		}

		// Extract node ID from key (format: "nodes/{id}")
		// Skip keys that contain additional path segments like "nodes/{id}/partitions"
		if len(keyStr) > len("nodes/") && keyStr[len("nodes/"):] != "" {
			// Check if this is a direct node entry (not a subdirectory)
			remainingPath := keyStr[len("nodes/"):]
			if len(remainingPath) > 0 && !strings.Contains(remainingPath, "/") {
				var node controller.NodeMetadata
				if err := json.Unmarshal(kv.Value, &node); err != nil {
					log.Printf("Failed to unmarshal node data for key %s: %v", keyStr, err)
					continue
				}

				// Only include alive nodes
				if node.Status == controller.Alive {
					nodeAddresses[node.ID] = node.HttpAddress
				}
			}
		}
	}

	lb.metadataLock.Lock()
	defer lb.metadataLock.Unlock()

	lb.metadataExpiry = time.Now().Add(lb.refreshInterval)
	lb.nodeAddressCache = nodeAddresses
	lb.partitionCache = partitions

	log.Printf("Refreshed metadata: %d partitions, %d nodes", len(partitions), len(nodeAddresses))
	return nil
}

func (lb *LoadBalancer) metadataRefresher() {
	ticker := time.NewTicker(lb.refreshInterval / 2)
	defer ticker.Stop()

	for range ticker.C {
		if err := lb.refreshMetadata(); err != nil {
			log.Printf("Background metadata refresh failed: %v", err)
		}
	}
}
