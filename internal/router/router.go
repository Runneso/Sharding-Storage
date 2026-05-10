package router

import (
	"HM5/internal/inmemory"
	"HM5/internal/protocol"
	"HM5/pkg/tcp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultRequestsBuffer = 1 << 13
	DefaultTimeoutDelay   = time.Second * 5
)

type Router struct {
	id       string
	hostname string
	port     int

	config   *inmemory.Config
	requests chan tcp.RequestInfo

	connectionManager *tcp.ConnectionManager
	deletedData       sync.Map
	handlers          map[string]func([]byte) (any, error)
	errorHandlers     map[string]func(error) any
}

func NewRouter(id string, hostname string, port int) *Router {
	requests := make(chan tcp.RequestInfo, DefaultRequestsBuffer)
	router := &Router{
		id:       id,
		hostname: hostname,
		port:     port,
		config:   inmemory.NewConfig(),
		requests: requests,
	}
	router.connectionManager = tcp.NewConnectionManager(router.SelfInfo(), requests, router.config)

	router.handlers = map[string]func([]byte) (any, error){
		protocol.TypeClientPutRequest:      router.handlerClientPutRequest,
		protocol.TypeClientDeleteRequest:   router.handlerClientDeleteRequest,
		protocol.TypeClientGetRequest:      router.handlerClientGetRequest,
		protocol.TypeClientDumpRequest:     router.handlerClientDumpRequest,
		protocol.TypeClientRangeGetRequest: router.handlerClientRangeGetRequest,

		protocol.TypeShardPutResponse:    router.handlerShardPutResponse,
		protocol.TypeShardDeleteResponse: router.handlerShardDeleteResponse,
		protocol.TypeShardGetResponse:    router.handlerShardGetResponse,
		protocol.TypeShardDumpResponse:   router.handlerShardDumpResponse,

		protocol.TypeClusterAddNode:     router.handlerClusterAddNode,
		protocol.TypeClusterRemoveNode:  router.handlerClusterRemoveNode,
		protocol.TypeClusterSetStrategy: router.handlerClusterSetStrategy,
		protocol.TypeClusterSetVNodes:   router.handlerClusterSetVNodes,
		protocol.TypeClusterSetRanges:   router.handlerClusterSetRanges,
		protocol.TypeClusterSetWeight:   router.handlerClusterSetWeight,
		protocol.TypeClusterMigrateData: router.handlerClusterMigrateData,
		protocol.TypeClusterInfo:        router.handlerClusterInfo,
		protocol.TypeRingInfoRequest:    router.handlerRingInfoRequest,
	}

	router.errorHandlers = map[string]func(error) any{
		protocol.TypeClientPutRequest:      router.handlerClientError,
		protocol.TypeClientDeleteRequest:   router.handlerClientError,
		protocol.TypeClientGetRequest:      router.handlerClientError,
		protocol.TypeClientDumpRequest:     router.handlerClientError,
		protocol.TypeClientRangeGetRequest: router.handlerClientError,
	}

	return router
}

func (router *Router) SelfInfo() protocol.NodeInfo {
	return protocol.NodeInfo{
		ID:       router.id,
		Hostname: router.hostname,
		Port:     router.port,
	}
}

func (router *Router) Start() error {
	address := net.JoinHostPort(router.hostname, strconv.Itoa(router.port))
	slog.Info("node started", "id", router.id, "addr", address)

	go router.workCycle()

	for {
		_, err := router.connectionManager.Accept()
		if err != nil {
			slog.Error("Can't accept connection", "error", err)
		}
	}
}

func (router *Router) workCycle() {
	for request := range router.requests {
		go router.handleRequest(request.Data, request.Connection)
	}
}

func (router *Router) handleRequest(data []byte, connection *tcp.Connection) {
	var env struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(data, &env); err != nil {
		response := protocol.NewBaseClientResponse(uuid.Nil, "", router.SelfInfo(), protocol.NewBadRequestError("bad json"))
		connection.Send(response)
		return
	}

	if handler, ok := router.handlers[env.Type]; ok {
		response, err := handler(data)
		if err == nil {
			if response != nil {
				connection.Send(response)
			}
			return
		}

		slog.Warn("handle request failed", "type", env.Type, "error", err)

		if errorHandler, ok := router.errorHandlers[env.Type]; ok {
			connection.Send(errorHandler(err))
		} else {
			slog.Warn("unknown handle error type", "type", env.Type)
			response := protocol.NewBaseClientResponse(uuid.Nil, "", router.SelfInfo(), protocol.NewUnknowError("unknown handle error type"))
			connection.Send(response)
		}
		return
	}

	slog.Warn("unknown handle type", "type", env.Type)
	response := protocol.NewBaseClientResponse(uuid.Nil, "", router.SelfInfo(), protocol.NewBadRequestError("unknown handle type"))
	connection.Send(response)
}

func (router *Router) handlerClusterAddNode(data []byte) (any, error) {
	var request protocol.ClusterAddNode
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal cluster add node request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "node", request.Node)

	router.config.AddNode(request.Node)
	router.connectionManager.ApplyClusterUpdate()

	config := router.config.AsDto()
	needMigrate := config.Strategy == protocol.ConsistentHashingMode || config.Strategy == protocol.HashBasedMode

	if needMigrate {
		err := router.migrate(nil)
		if err != nil {
			return protocol.NewClusterAck(request.RequestID, err), nil
		}
	}

	response := protocol.NewClusterAck(request.RequestID, nil)
	return response, nil
}

func (router *Router) handlerClusterRemoveNode(data []byte) (any, error) {
	var request protocol.ClusterRemoveNode
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal cluster remove node request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "node", request.Node)
	config := router.config.AsDto()
	needMigrate := config.Strategy == protocol.ConsistentHashingMode || config.Strategy == protocol.HashBasedMode
	var result any

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
	defer cancel()
	requestID := uuid.New()
	router.connectionManager.CreateResponse(requestID, ctx, cancel)
	response := protocol.NewShardDumpRequest(requestID)
	victim := router.connectionManager.GetConnectionByNodeId(request.Node.ID)
	go router.retrying(ctx, victim, response)
	<-ctx.Done()
	shardResult, err := router.connectionManager.GetResponse(requestID)
	if err != nil {
		return protocol.NewClusterAck(request.RequestID, err), nil
	}
	result = shardResult

	for k, v := range shardResult.(protocol.ShardDumpResponse).Dump {
		router.deletedData.Store(k, v)
	}

	router.config.RemoveNode(request.Node)
	router.connectionManager.ApplyClusterUpdate()

	if needMigrate {
		err := router.migrate(result.(protocol.ShardDumpResponse).Dump)
		if err != nil {
			return protocol.NewClusterAck(request.RequestID, err), nil
		}
	}

	return protocol.NewClusterAck(request.RequestID, nil), nil
}

func (router *Router) handlerClusterSetStrategy(data []byte) (any, error) {
	var request protocol.ClusterSetStrategy
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal cluster set strategy request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "strategy", request.Strategy)

	err := router.config.SetStrategy(request.Strategy)

	response := protocol.NewClusterAck(request.RequestID, err)
	return response, nil
}

func (router *Router) handlerClusterSetVNodes(data []byte) (any, error) {
	var request protocol.ClusterSetVNodes
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal cluster set vnodes request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "count", request.Count)

	err := router.config.SetVNodes(request.Count)
	config := router.config.AsDto()
	needMigrate := err == nil && config.Strategy == protocol.ConsistentHashingMode

	if needMigrate {
		err := router.migrate(nil)
		if err != nil {
			return protocol.NewClusterAck(request.RequestID, err), nil
		}
	}

	response := protocol.NewClusterAck(request.RequestID, err)
	return response, nil
}

func (router *Router) handlerClusterSetRanges(data []byte) (any, error) {
	var request protocol.ClusterSetRanges
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal cluster set ranges request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "ranges", request.Boundaries)

	err := router.config.SetRanges(request.Boundaries)

	response := protocol.NewClusterAck(request.RequestID, err)
	return response, nil
}

func (router *Router) handlerClusterSetWeight(data []byte) (any, error) {
	var request protocol.ClusterSetWeight
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal cluster set weight request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "node_id", request.NodeID, "weight", request.Weight)

	err := router.config.SetWeight(request.NodeID, request.Weight)
	config := router.config.AsDto()
	needMigrate := err == nil && config.Strategy == protocol.ConsistentHashingMode

	if needMigrate {
		err := router.migrate(nil)
		if err != nil {
			return protocol.NewClusterAck(request.RequestID, err), nil
		}
	}

	response := protocol.NewClusterAck(request.RequestID, err)
	return response, nil
}

func (router *Router) handlerClusterMigrateData(data []byte) (any, error) {
	var request protocol.ClusterMigrateData
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal cluster migrate data request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID)

	err := router.migrate(nil)

	response := protocol.NewClusterAck(request.RequestID, err)
	return response, nil
}

func (router *Router) handlerClusterInfo(data []byte) (any, error) {
	var request protocol.ClusterInfo
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal cluster info request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID)

	response := protocol.NewClusterState(request.RequestID, router.config.AsDto())
	return response, nil
}

func (router *Router) handlerShardPutResponse(data []byte) (any, error) {
	var request protocol.ShardPutResponse
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal shard put response request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID)
	router.connectionManager.SetResponse(request.RequestID, request)

	return nil, nil
}

func (router *Router) handlerShardDeleteResponse(data []byte) (any, error) {
	var request protocol.ShardDeleteResponse
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal shard delete response request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID)
	router.connectionManager.SetResponse(request.RequestID, request)

	return nil, nil
}

func (router *Router) handlerShardGetResponse(data []byte) (any, error) {
	var request protocol.ShardGetResponse
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal shard get response request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "value", request.Value, "found", request.Found)
	router.connectionManager.SetResponse(request.RequestID, request)

	return nil, nil
}

func (router *Router) handlerShardDumpResponse(data []byte) (any, error) {
	var request protocol.ShardDumpResponse
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal shard dump response request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "dump", request.Dump)
	router.connectionManager.SetResponse(request.RequestID, request)

	return nil, nil
}

func (router *Router) handlerClientPutRequest(data []byte) (any, error) {
	var request protocol.ClientPutRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal client put request request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "key", request.Key, "value", request.Value)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
	defer cancel()

	router.connectionManager.CreateResponse(request.RequestID, ctx, cancel)
	response := protocol.NewShardPutRequest(request.RequestID, request.Key, request.Value)
	victim := router.connectionManager.FindShard(request.Key)
	go router.retrying(ctx, victim, response)

	<-ctx.Done()

	result, err := router.connectionManager.GetResponse(request.RequestID)

	if err != nil {
		return nil, err
	}

	return protocol.NewClientPutResponse(
		result.(protocol.ShardPutResponse).RequestID,
		router.SelfInfo(),
		nil,
	), nil
}

func (router *Router) handlerClientDeleteRequest(data []byte) (any, error) {
	var request protocol.ClientDeleteRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal client delete request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "key", request.Key)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
	defer cancel()

	router.connectionManager.CreateResponse(request.RequestID, ctx, cancel)
	response := protocol.NewShardDeleteRequest(request.RequestID, request.Key)
	victim := router.connectionManager.FindShard(request.Key)
	go router.retrying(ctx, victim, response)

	<-ctx.Done()

	result, err := router.connectionManager.GetResponse(request.RequestID)

	if err != nil {
		return nil, err
	}

	return protocol.NewClientDeleteResponse(
		result.(protocol.ShardDeleteResponse).RequestID,
		router.SelfInfo(),
		nil,
	), nil
}

func (router *Router) handlerClientGetRequest(data []byte) (any, error) {
	var request protocol.ClientGetRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal client get request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "key", request.Key)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
	defer cancel()

	router.connectionManager.CreateResponse(request.RequestID, ctx, cancel)
	response := protocol.NewShardGetRequest(request.RequestID, request.Key)
	victim := router.connectionManager.FindShard(request.Key)
	go router.retrying(ctx, victim, response)

	<-ctx.Done()

	result, err := router.connectionManager.GetResponse(request.RequestID)

	if err != nil {
		return nil, err
	}

	return protocol.NewClientGetResponse(
		result.(protocol.ShardGetResponse).RequestID,
		router.SelfInfo(),
		nil,
		result.(protocol.ShardGetResponse).Value,
		result.(protocol.ShardGetResponse).Found,
	), nil
}

func (router *Router) handlerClientDumpRequest(data []byte) (any, error) {
	var request protocol.ClientDumpRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal client dump request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "node_id", request.NodeID)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
	defer cancel()

	router.connectionManager.CreateResponse(request.RequestID, ctx, cancel)
	response := protocol.NewShardDumpRequest(request.RequestID)
	victim := router.connectionManager.GetConnectionByNodeId(request.NodeID)
	go router.retrying(ctx, victim, response)

	<-ctx.Done()

	result, err := router.connectionManager.GetResponse(request.RequestID)

	if err != nil {
		return nil, err
	}

	return protocol.NewClientDumpResponse(
		result.(protocol.ShardDumpResponse).RequestID,
		router.SelfInfo(),
		nil,
		result.(protocol.ShardDumpResponse).Dump,
	), nil
}

func (router *Router) handlerClientRangeGetRequest(data []byte) (any, error) {
	var request protocol.ClientRangeGetRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal client range get request: %w", protocol.NewBadRequestError("bad json"))
	}

	config := router.config.AsDto()

	if config.Strategy != protocol.RangeBasedMode {
		return nil, protocol.NewBadRequestError("range get request is only supported in range based mode")
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "left_key", request.LeftKey, "right_key", request.RightKey)

	nodes := make([]protocol.NodeInfo, 0, len(config.Nodes))
	for _, node := range config.Nodes {
		nodes = append(nodes, node)
	}

	n := len(nodes)

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	ctxs := make(map[uuid.UUID]context.Context, len(nodes))

	if request.LeftKey < config.Ranges[0] {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
		defer cancel()
		requestID := uuid.New()
		ctxs[requestID] = ctx
		router.connectionManager.CreateResponse(requestID, ctx, cancel)
		response := protocol.NewShardDumpRequest(requestID)
		victim := router.connectionManager.GetConnectionByNodeId(nodes[0].ID)
		go router.retrying(ctx, victim, response)
	}

	if request.RightKey >= config.Ranges[n-2] {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
		defer cancel()
		requestID := uuid.New()
		ctxs[requestID] = ctx
		router.connectionManager.CreateResponse(requestID, ctx, cancel)
		response := protocol.NewShardDumpRequest(requestID)
		victim := router.connectionManager.GetConnectionByNodeId(nodes[n-1].ID)
		go router.retrying(ctx, victim, response)
	}

	for index := 1; index < n-1; index++ {
		prev := config.Ranges[index-1]
		next := config.Ranges[index]

		if request.RightKey < prev || request.LeftKey >= next {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
		defer cancel()
		requestID := uuid.New()
		ctxs[requestID] = ctx
		router.connectionManager.CreateResponse(requestID, ctx, cancel)
		response := protocol.NewShardDumpRequest(requestID)
		victim := router.connectionManager.GetConnectionByNodeId(nodes[index].ID)
		go router.retrying(ctx, victim, response)
	}

	totalDump := make(map[string]string)

	for requestID, ctx := range ctxs {
		<-ctx.Done()
		result, err := router.connectionManager.GetResponse(requestID)
		if err != nil {
			return nil, err
		}

		for k, v := range result.(protocol.ShardDumpResponse).Dump {
			if k >= request.LeftKey && k < request.RightKey {
				totalDump[k] = v
			}
		}
	}

	return protocol.NewClientRangeGetResponse(
		request.RequestID,
		router.SelfInfo(),
		nil,
		totalDump,
	), nil
}

func (router *Router) handlerRingInfoRequest(data []byte) (any, error) {
	var request protocol.RingInfoRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal client range get request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID)

	config := router.config.AsDto()
	response := protocol.NewRingInfoResponse(request.RequestID, router.connectionManager.BuildRing(config))

	return response, nil
}

func (router *Router) handlerClientError(error error) any {
	response := protocol.NewBaseClientResponse(uuid.Nil, "", router.SelfInfo(), error)
	return response
}

func (router *Router) migrate(baseDump map[string]string) error {
	if baseDump == nil {
		baseDump = make(map[string]string)
	}
	router.deletedData.Range(
		func(key, value interface{}) bool {
			baseDump[key.(string)] = value.(string)
			return true
		},
	)
	for key, _ := range baseDump {
		router.deletedData.Delete(key)
	}
	config := router.config.AsDto()
	totalKeys := len(baseDump)
	movedKeys := len(baseDump)
	startTime := time.Now()

	// Dump
	ctxs := make(map[uuid.UUID]context.Context, len(config.Nodes))
	prevShard := make(map[uuid.UUID]string)
	toDelete := make(map[*tcp.Connection][]string, len(config.Nodes))
	toPut := make(map[*tcp.Connection]map[string]string, len(config.Nodes))

	for k, v := range baseDump {
		newConnection := router.connectionManager.FindShard(k)
		if _, ok := toPut[newConnection]; !ok {
			toPut[newConnection] = make(map[string]string)
		}
		toPut[newConnection][k] = v
	}

	for _, node := range config.Nodes {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
		defer cancel()
		requestID := uuid.New()
		prevShard[requestID] = node.ID
		ctxs[requestID] = ctx
		router.connectionManager.CreateResponse(requestID, ctx, cancel)
		response := protocol.NewShardDumpRequest(requestID)
		victim := router.connectionManager.GetConnectionByNodeId(node.ID)
		go router.retrying(ctx, victim, response)
	}

	for requestID, ctx := range ctxs {
		<-ctx.Done()
		result, err := router.connectionManager.GetResponse(requestID)
		if err != nil {
			return err
		}

		shardResponse := result.(protocol.ShardDumpResponse)
		totalKeys += len(shardResponse.Dump)
		for k, v := range shardResponse.Dump {
			newConnection := router.connectionManager.FindShard(k)
			oldConnection := router.connectionManager.GetConnectionByNodeId(prevShard[requestID])
			if newConnection != oldConnection {
				if _, ok := toPut[newConnection]; !ok {
					toPut[newConnection] = make(map[string]string)
				}
				toPut[newConnection][k] = v
				toDelete[oldConnection] = append(toDelete[oldConnection], k)
				movedKeys++
			}
		}
	}
	ctxs = make(map[uuid.UUID]context.Context, len(config.Nodes))

	// Put for new
	for connection, puts := range toPut {
		for k, v := range puts {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
			defer cancel()
			requestID := uuid.New()
			ctxs[requestID] = ctx
			router.connectionManager.CreateResponse(requestID, ctx, cancel)
			response := protocol.NewShardPutRequest(requestID, k, v)
			go router.retrying(ctx, connection, response)
		}
	}

	for requestID, ctx := range ctxs {
		<-ctx.Done()
		_, err := router.connectionManager.GetResponse(requestID)
		if err != nil {
			return err
		}
	}
	// Delete for old
	ctxs = make(map[uuid.UUID]context.Context, len(config.Nodes))
	for connection, deletes := range toDelete {
		for _, del := range deletes {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeoutDelay)
			defer cancel()
			requestID := uuid.New()
			ctxs[requestID] = ctx
			router.connectionManager.CreateResponse(requestID, ctx, cancel)
			response := protocol.NewShardDeleteRequest(requestID, del)
			go router.retrying(ctx, connection, response)
		}
	}

	for requestID, ctx := range ctxs {
		<-ctx.Done()
		_, err := router.connectionManager.GetResponse(requestID)
		if err != nil {
			return err
		}
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	movedPercent := 0.0
	if totalKeys > 0 {
		movedPercent = float64(movedKeys) * 100.0 / float64(totalKeys)
	}

	slog.Info("node migrate finished", "totalKeys", totalKeys, "movedKeys", movedKeys, "movedPercent", movedPercent, "duration", duration)
	return nil
}

func (router *Router) retrying(ctx context.Context, connection *tcp.Connection, request any) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	if connection == nil {
		return
	}
	connection.Send(request)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if connection == nil {
				return
			}
			connection.Send(request)
		}
	}
}
