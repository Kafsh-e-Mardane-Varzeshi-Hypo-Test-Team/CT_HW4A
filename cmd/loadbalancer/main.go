package main

import (
	"log"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/loadbalancer"
)

func main() {
	// etcd endpoints - these should match your etcd cluster configuration
	etcdEndpoints := []string{
		"http://etcd-1:2379",
		"http://etcd-2:2379",
		"http://etcd-3:2379",
	}

	lb := loadbalancer.NewLoadBalancer(etcdEndpoints)

	if err := lb.Run(":9001"); err != nil {
		log.Fatalf("Failed to start load balancer: %v", err)
	}
}
