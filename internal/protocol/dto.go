package protocol

const (
	RangeBasedMode        = "range"
	HashBasedMode         = "hash"
	ConsistentHashingMode = "consistent"
)

var Strategies = map[string]struct{}{RangeBasedMode: {}, HashBasedMode: {}, ConsistentHashingMode: {}}

type Dump map[string]string

type Boundaries []string

type NodeInfo struct {
	ID       string `json:"node_id"`
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
}

type ClusterConfig struct {
	Nodes    map[string]NodeInfo `json:"nodes"`
	Weights  map[string]int      `json:"weights"` // NodeID -> Weight (default = 1)
	Strategy string              `json:"strategy"`
	VNodes   int                 `json:"v_nodes"`
	Ranges   []string            `json:"ranges"`
}

type RingNode struct {
	Hash uint64   `json:"hash"`
	Node NodeInfo `json:"node"`
}
