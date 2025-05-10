package node

type Node struct {
	// TODO
}

func (n *Node) Set(key, value string) (NodeSetResp, error) {
	// TODO
	return NodeSetResp{}, nil
}

func (n *Node) Get(key string) (NodeGetResp, error) {
	// TODO
	return NodeGetResp{}, nil
}

func (n *Node) Delete(key string) (NodeDeleteResp, error) {
	// TODO
	return NodeDeleteResp{}, nil
}

func Start() {
	replicasInitialization()
	tcpInitialization()
	go heartbeat()

}

func replicasInitialization() {
	// TODO
}

func tcpInitialization() {
	// TODO
}

func heartbeat() {
	// TODO
}
