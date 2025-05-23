package controller

import (
	"log"

	"github.com/gin-gonic/gin"
)

func (c *Controller) setupRoutes() {
	c.ginEngine.GET("/metadata", c.handleGetMetadata)

	c.ginEngine.POST("/nodes", c.handleRegisterNode)
}

func (c *Controller) Run(addr string) error {
	if err := c.RegisterNode(1); err != nil {
		log.Fatalf("Failed to create initial node: %v", err)
	}
	return c.ginEngine.Run(addr)
}

func (c *Controller) handleGetMetadata(ctx *gin.Context) {

}

func (c *Controller) handleRegisterNode(ctx *gin.Context) {

}

func (c *Controller) handleRemoveNode(ctx *gin.Context) {

}

func (c *Controller) handleSetLeader(ctx *gin.Context) {

}

func (c *Controller) handleRebalance(ctx *gin.Context) {

}

func (c *Controller) handleMoveReplica(ctx *gin.Context) {

}

func (c *Controller) handleReadyCheck(ctx *gin.Context) {

}
