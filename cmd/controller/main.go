package main

import (
	"log"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/controller"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW4A/internal/cluster/controller/docker"
)

func main() {
	dockerClient, err := docker.NewDockerClient()
	if err != nil {
		panic("Failed to create Docker client")
	}
	controller := controller.NewController(dockerClient, 3, 2, "ct_hw4a_temp", "ct_hw4a-node:latest")
	if err := controller.Start(":8080"); err != nil {
		log.Fatalf("Failed to start controller: %v", err)
	}
}
