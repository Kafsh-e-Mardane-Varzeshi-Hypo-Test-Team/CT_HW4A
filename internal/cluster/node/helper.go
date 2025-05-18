package node

import (
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
)

// TODO(discuss): i guess it's not clean!
type RequestToFollowerNodes struct {
	replica.ReplicaLog
}

type SetRequest struct {
	timestamp int64
	Key string
	Value string
}
