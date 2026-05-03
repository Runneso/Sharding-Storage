package inmemory

import (
	"HM5/internal/protocol"
	"sync"
)

const (
	DefaultStrategy = protocol.HashBasedMode
	DefaultVNodes   = 3
)

type Config struct {
	mu       sync.RWMutex
	nodes    map[string]protocol.NodeInfo // nodeID -> node
	weights  map[string]int
	strategy string
	VNodes   int
	Ranges   []string
}

func NewConfig() *Config {
	return &Config{
		nodes:    make(map[string]protocol.NodeInfo),
		weights:  make(map[string]int),
		strategy: DefaultStrategy,
		VNodes:   DefaultVNodes,
	}
}

func (config *Config) AddNode(node protocol.NodeInfo) {
	config.mu.Lock()
	defer config.mu.Unlock()
	config.nodes[node.ID] = node
	config.weights[node.ID] = 1
}

func (config *Config) RemoveNode(node protocol.NodeInfo) {
	config.mu.Lock()
	defer config.mu.Unlock()
	delete(config.nodes, node.ID)
	delete(config.weights, node.ID)
}

func (config *Config) SetWeight(nodeID string, weight int) error {
	if weight < 1 {
		return protocol.NewInvalidClusterConfigError("Weight must be at least 1!")
	}

	config.mu.Lock()
	defer config.mu.Unlock()
	if _, exists := config.nodes[nodeID]; !exists {
		return protocol.NewInvalidClusterConfigError("Node is not present!")
	}
	config.weights[nodeID] = weight
	return nil
}

func (config *Config) SetStrategy(strategy string) error {
	if _, ok := protocol.Strategies[strategy]; !ok {
		return protocol.NewInvalidClusterConfigError("Invalid strategy!")
	}

	config.mu.Lock()
	defer config.mu.Unlock()
	config.strategy = strategy
	return nil
}

func (config *Config) SetVNodes(count int) error {
	if count < 1 {
		return protocol.NewInvalidClusterConfigError("VNodes count must be at least 1!")
	}
	config.mu.Lock()
	defer config.mu.Unlock()
	config.VNodes = count
	return nil
}

func (config *Config) SetRanges(boundaries []string) error {
	config.mu.Lock()
	defer config.mu.Unlock()

	if len(boundaries) != len(config.nodes)-1 {
		return protocol.NewInvalidClusterConfigError("Invalid boundaries length!")
	}

	for index := 0; index < len(boundaries)-1; index++ {
		if boundaries[index] >= boundaries[index+1] {
			return protocol.NewInvalidClusterConfigError("Invalid boundaries!")
		}
	}

	config.Ranges = boundaries
	return nil
}

func (config *Config) AsDto() protocol.ClusterConfig {
	config.mu.RLock()
	defer config.mu.RUnlock()

	nodes := make(map[string]protocol.NodeInfo, len(config.nodes))
	for nodeID, node := range config.nodes {
		nodes[nodeID] = node
	}

	weights := make(map[string]int, len(config.weights))
	for nodeID, weight := range config.weights {
		weights[nodeID] = weight
	}

	ranges := make([]string, len(config.Ranges))
	copy(ranges, config.Ranges)

	return protocol.ClusterConfig{
		Nodes:    nodes,
		Weights:  weights,
		Strategy: config.strategy,
		VNodes:   config.VNodes,
		Ranges:   ranges,
	}
}
