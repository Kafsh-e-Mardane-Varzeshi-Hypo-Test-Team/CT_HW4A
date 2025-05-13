package replica

type Replica struct {
	// TODO
}

type ReplicaResp struct {
	// TODO
	// timestamp
	// action: set, delete
	// key
	// value: -
}

func (r *Replica) Set(key, value string) (ReplicaResp, error) {
	// TODO
	return ReplicaResp{}, nil
}

func (r *Replica) Get(key string) (ReplicaResp, error) {
	// TODO
	return ReplicaResp{}, nil
}

func (r *Replica) Delete(key string) (ReplicaResp, error) {
	// TODO
	return ReplicaResp{}, nil
}
