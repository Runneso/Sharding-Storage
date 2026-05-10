package tcp

import (
	"HM5/internal/inmemory"
	"HM5/internal/protocol"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	MaxAttempts            = 30 // 15 seconds
	DefaultReconnectPeriod = 500 * time.Millisecond
)

type ConnectionManager struct {
	mu         sync.RWMutex
	self       protocol.NodeInfo
	listener   net.Listener
	requests   chan RequestInfo
	responses  map[uuid.UUID]*ResponseInfo
	peers      map[string]*Connection // address -> connection
	strategies map[string]func(protocol.ClusterConfig, string) *Connection
	config     *inmemory.Config
}

func NewConnectionManager(self protocol.NodeInfo, requests chan RequestInfo, config *inmemory.Config) *ConnectionManager {
	listener, _ := net.Listen(DefaultProtocol, net.JoinHostPort(self.Hostname, strconv.Itoa(self.Port)))
	manager := &ConnectionManager{
		self:      self,
		requests:  requests,
		config:    config,
		responses: make(map[uuid.UUID]*ResponseInfo),
		peers:     make(map[string]*Connection),
		listener:  listener,
	}
	manager.strategies = map[string]func(protocol.ClusterConfig, string) *Connection{
		protocol.HashBasedMode:         manager.hashBasedStrategy,
		protocol.RangeBasedMode:        manager.rangeBasedStrategy,
		protocol.ConsistentHashingMode: manager.consistentHashingStrategy,
	}

	return manager
}

func (manager *ConnectionManager) Accept() (*Connection, error) {
	conn, err := manager.listener.Accept()

	if err != nil {
		return nil, err
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	connection := NewConnection(
		conn,
		manager.requests,
	)

	return connection, nil
}

func (manager *ConnectionManager) ApplyClusterUpdate() {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	for _, connection := range manager.peers {
		connection.Close()
	}

	manager.peers = make(map[string]*Connection)
	nodes := manager.config.AsDto().Nodes

	for _, node := range nodes {
		address := net.JoinHostPort(node.Hostname, strconv.Itoa(node.Port))
		conn, err := net.Dial(DefaultProtocol, address)
		if err != nil {
			slog.Warn("Unable to connect to node", "address", address, "error", err)
			continue
		}
		manager.peers[address] = NewConnection(conn, manager.requests)
		go manager.startReconnectLoop(manager.peers[address])
	}
}

func (manager *ConnectionManager) CreateResponse(requestID uuid.UUID, ctx context.Context, ready context.CancelFunc) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.responses[requestID] = &ResponseInfo{
		Data:  nil,
		Ready: ready,
		Ctx:   ctx,
	}
}

func (manager *ConnectionManager) SetResponse(requestID uuid.UUID, response any) {
	manager.mu.Lock()

	requestInfo, ok := manager.responses[requestID]
	if !ok {
		manager.mu.Unlock()
		return
	}

	requestInfo.Data = response
	cancelFunc := requestInfo.Ready

	manager.mu.Unlock()

	cancelFunc()
}

func (manager *ConnectionManager) GetResponse(requestID uuid.UUID) (any, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	requestInfo := manager.responses[requestID]

	if errors.Is(requestInfo.Ctx.Err(), context.DeadlineExceeded) {
		return nil, protocol.NewTimeoutError("Request timed out!")
	}

	data := requestInfo.Data
	delete(manager.responses, requestID)
	return data, nil
}

func (manager *ConnectionManager) FindShard(key string) *Connection {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	config := manager.config.AsDto()
	return manager.strategies[config.Strategy](config, key)
}

func (manager *ConnectionManager) rangeBasedStrategy(config protocol.ClusterConfig, key string) *Connection {
	nodes := make([]protocol.NodeInfo, 0, len(config.Nodes))

	for _, node := range config.Nodes {
		nodes = append(nodes, node)
	}

	n := len(nodes)

	sort.Slice(
		nodes, func(i, j int) bool {
			return nodes[i].ID < nodes[j].ID
		},
	)

	for index := 0; index < n-1; index++ {
		if config.Ranges[index] > key {
			address := net.JoinHostPort(nodes[index].Hostname, strconv.Itoa(nodes[index].Port))
			return manager.peers[address]
		}
	}

	address := net.JoinHostPort(nodes[n-1].Hostname, strconv.Itoa(nodes[n-1].Port))
	return manager.peers[address]
}

func (manager *ConnectionManager) hashBasedStrategy(config protocol.ClusterConfig, key string) *Connection {
	nodes := make([]protocol.NodeInfo, 0, len(config.Nodes))
	for _, node := range config.Nodes {
		nodes = append(nodes, node)
	}

	n := len(nodes)

	sort.Slice(
		nodes, func(i, j int) bool {
			return nodes[i].ID < nodes[j].ID
		},
	)

	index := manager.hash(key) % uint64(n)
	address := net.JoinHostPort(nodes[index].Hostname, strconv.Itoa(nodes[index].Port))
	return manager.peers[address]
}

func (manager *ConnectionManager) consistentHashingStrategy(config protocol.ClusterConfig, key string) *Connection {
	ring := manager.BuildRing(config)
	keyHash := manager.hash(key)

	index := sort.Search(len(ring), func(i int) bool {
		return ring[i].Hash >= keyHash
	})

	if index == len(ring) {
		index = 0
	}

	node := ring[index].Node
	address := net.JoinHostPort(node.Hostname, strconv.Itoa(node.Port))

	return manager.peers[address]
}

func (manager *ConnectionManager) BuildRing(config protocol.ClusterConfig) []protocol.RingNode {
	ring := make([]protocol.RingNode, 0, len(config.Nodes)*config.VNodes)
	for _, node := range config.Nodes {
		for index := 0; index < config.VNodes*config.Weights[node.ID]; index++ {
			ring = append(
				ring, protocol.RingNode{
					Hash: manager.hash(node.ID + "#" + strconv.Itoa(index)),
					Node: node,
				},
			)
		}
	}

	sort.Slice(ring, func(i, j int) bool {
		return ring[i].Hash < ring[j].Hash
	})

	return ring
}

func (manager *ConnectionManager) GetConnectionByNodeId(nodeID string) *Connection {
	config := manager.config.AsDto()
	for _, node := range config.Nodes {
		if node.ID == nodeID {
			address := net.JoinHostPort(node.Hostname, strconv.Itoa(node.Port))
			manager.mu.RLock()
			result := manager.peers[address]
			manager.mu.RUnlock()
			return result
		}
	}
	return nil
}

func (manager *ConnectionManager) startReconnectLoop(connection *Connection) {
	ticker := time.NewTicker(DefaultReconnectPeriod)
	defer ticker.Stop()

	fails := 0

	for {
		select {
		case <-connection.done:
			return

		case <-ticker.C:
			if fails >= MaxAttempts {
				return
			}

			connection.mu.Lock()
			alive := connection.conn != nil
			connection.mu.Unlock()
			if alive {
				continue
			}

			if err := connection.Reconnect(); err != nil {
				fails++
				continue
			}
			fails = 0
		}
	}
}

func (manager *ConnectionManager) hash(key string) uint64 {
	hash := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(hash[:8])
}
