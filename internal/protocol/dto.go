package protocol

const (
	RangeBasedMode        = "range"
	HashBasedMode         = "hash"
	ConsistentHashingMode = "consistent"
)

type Dump map[string]string

type Boundaries []Boundary

type Boundary struct {
	Left, Right string
}

type NodeInfo struct {
	ID       string `json:"node_id"`
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
}
