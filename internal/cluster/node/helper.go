package node

import (
	"encoding/gob"
)

func init() {
	gob.Register(Message{})
	gob.Register(Response{})
}

type MessageType string

const (
	Set    MessageType = "SET"
	Get    MessageType = "GET"
	Delete MessageType = "DELETE"
)

type Message struct {
	Type        MessageType
	PartitionId int
	Timestamp   int64
	Key         string
	Value       string
}

type Response struct {
	Error error
	Value string
}

type Heartbeat struct {
	NodeId int
}
