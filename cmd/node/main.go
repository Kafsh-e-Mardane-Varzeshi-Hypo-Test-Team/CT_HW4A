package main

import (
	"log"
	"os"
	"strconv"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/node"
)

func main() {
	err := loadConfig()
	if err != nil {
		log.Fatalf("[node.Start] can not load config due to: %v", err)
	}

	id, err := strconv.Atoi(os.Getenv("NODE-ID"))
	if err != nil {
		log.Fatalf("[main (node)] failed to get 'NODE-ID' env variable: %v", err)
	}

	// TODO: read etcd endpoints from env file or sth
	etcdEndpoints := []string{"http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379"}

	n := node.NewNode(id, etcdEndpoints)
	n.Start()
}

func loadConfig() error {
	// TODO
	return nil
}
