package main

import (
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/controller"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/controller/docker"
)

func main() {
	dockerClient, err := docker.NewDockerClient()
	if err != nil {
		panic("Failed to create Docker client")
	}

	// TODO: read etcd endpoints from env file or sth
	etcdEndpoints := []string{"http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379"}

	controller := controller.NewController(dockerClient, 3, 2, "ct_hw4a_temp", "ct_hw4a-node:latest", etcdEndpoints)
	controller.Start(":8080")
}
