package main

import (
	"log"
	"os"
	"strconv"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/node"
)

func main() {
	id, err := strconv.Atoi(os.Getenv("NODE-ID"))
	if err != nil {
		log.Fatalf("[main (node)] failed to get 'NODE-ID' env variable: %v", err)
	}

	n := node.NewNode(id)
	if err := n.Start(); err != nil {
		log.Fatalf("[main (node)] failed to start node: %v", err)
	}
}
