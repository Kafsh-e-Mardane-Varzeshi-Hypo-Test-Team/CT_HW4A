package main

import (
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller/api"
	"github.com/gin-gonic/gin"
)

func main() {
	controller := controller.NewController(1, 1)
	controller.Start()

	r := gin.Default()
	r.POST("/node/register", api.RegisterNode)
}
