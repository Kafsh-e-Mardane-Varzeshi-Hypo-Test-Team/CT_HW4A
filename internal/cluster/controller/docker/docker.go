package docker

import (
	"context"
	"errors"
	"log"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

type DockerClient struct {
	cli     *client.Client
	respIDs map[string]string
}

func NewDockerClient() (*DockerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Printf("docker::NewDockerClient: Failed to create docker client\n")
		return nil, errors.New("failed to create docker client")
	}

	return &DockerClient{cli: cli, respIDs: make(map[string]string)}, nil
}

func (d *DockerClient) CreateNodeContainer(imageName, nodeName, networkName string, exposedPort nat.Port) error {
	ctx := context.Background()

	resp, err := d.cli.ContainerCreate(ctx, &container.Config{
		Image: imageName,
		ExposedPorts: nat.PortSet{
			exposedPort: {},
		},
	}, &container.HostConfig{
		NetworkMode: container.NetworkMode(networkName),
	}, nil, nil, nodeName)
	if err != nil {
		log.Printf("docker::CreateNodeContainer: Failed to create container %s\n", nodeName)
		return errors.New("failed to create node container")
	}
	log.Printf("docker::CreateNodeContainer: %s created successfully\n", nodeName)

	if err := d.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		log.Printf("docker::CreateNodeContainer: Failed to start container %s\n", nodeName)
		return errors.New("failed to start node container")
	}
	log.Printf("docker::CreateNodeContainer: %s started successfully\n", nodeName)

	d.respIDs[nodeName] = resp.ID
	return nil
}
