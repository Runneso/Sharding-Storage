package shard

import (
	"HM5/internal/inmemory"
	"HM5/internal/protocol"
	"HM5/pkg/tcp"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
)

const (
	DefaultRequestsBuffer = 1 << 13
)

type Shard struct {
	id       string
	hostname string
	port     int

	storage       *inmemory.Storage
	deduplication *inmemory.Deduplication
	requests      chan tcp.RequestInfo

	connectionManager *tcp.ConnectionManager
	handlers          map[string]func([]byte) (any, error)
}

func NewShard(id string, hostname string, port int) *Shard {
	requests := make(chan tcp.RequestInfo, DefaultRequestsBuffer)
	shard := &Shard{
		id:            id,
		hostname:      hostname,
		storage:       inmemory.NewStorage(),
		deduplication: inmemory.NewDeduplication(),
		port:          port,
		requests:      requests,
	}
	shard.connectionManager = tcp.NewConnectionManager(shard.SelfInfo(), requests, inmemory.NewConfig())

	shard.handlers = map[string]func([]byte) (any, error){
		protocol.TypeShardPutRequest:    shard.handlerShardPutRequest,
		protocol.TypeShardDeleteRequest: shard.handlerShardDeleteRequest,
		protocol.TypeShardGetRequest:    shard.handlerShardGetRequest,
		protocol.TypeShardDumpRequest:   shard.handlerShardDumpRequest,
	}

	return shard
}

func (shard *Shard) SelfInfo() protocol.NodeInfo {
	return protocol.NodeInfo{
		ID:       shard.id,
		Hostname: shard.hostname,
		Port:     shard.port,
	}
}

func (shard *Shard) Start() error {
	shard.deduplication.StartVacuum()
	address := net.JoinHostPort(shard.hostname, strconv.Itoa(shard.port))
	slog.Info("node started", "id", shard.id, "addr", address)

	go shard.workCycle()

	for {
		_, err := shard.connectionManager.Accept()
		if err != nil {
			slog.Error("Can't accept connection", "error", err)
		}
	}
}

func (shard *Shard) workCycle() {
	for request := range shard.requests {
		go shard.handleRequest(request.Data, request.Connection)
	}
}

func (shard *Shard) handleRequest(data []byte, connection *tcp.Connection) {
	var env struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(data, &env); err != nil {
		return
	}

	if handler, ok := shard.handlers[env.Type]; ok {
		response, err := handler(data)
		if err == nil {
			if response != nil {
				connection.Send(response)
			}
			return
		}

		slog.Warn("handle request failed", "type", env.Type, "error", err)
		return
	}

	slog.Warn("unknown handle type", "type", env.Type)
}

func (shard *Shard) handlerShardPutRequest(data []byte) (any, error) {
	var request protocol.ShardPutRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal shard put request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "key", request.Key, "value", request.Value)

	isFresh := shard.deduplication.AddIfAbsent(request.RequestID)

	if !isFresh {
		return protocol.NewShardPutResponse(request.RequestID, shard.SelfInfo()), nil
	}

	shard.storage.Put(request.Key, request.Value)

	return protocol.NewShardPutResponse(request.RequestID, shard.SelfInfo()), nil
}

func (shard *Shard) handlerShardDeleteRequest(data []byte) (any, error) {
	var request protocol.ShardDeleteRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal shard delete request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "key", request.Key)

	isFresh := shard.deduplication.AddIfAbsent(request.RequestID)

	if !isFresh {
		return protocol.NewShardDeleteResponse(request.RequestID, shard.SelfInfo()), nil
	}

	shard.storage.Delete(request.Key)

	return protocol.NewShardDeleteResponse(request.RequestID, shard.SelfInfo()), nil
}

func (shard *Shard) handlerShardGetRequest(data []byte) (any, error) {
	var request protocol.ShardGetRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal shard get request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID, "key", request.Key)

	value, found := shard.storage.Get(request.Key)

	return protocol.NewShardGetResponse(request.RequestID, value, found, shard.SelfInfo()), nil
}

func (shard *Shard) handlerShardDumpRequest(data []byte) (any, error) {
	var request protocol.ShardDumpRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("unmarshal shard dump request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID)

	dump := shard.storage.Dump()

	return protocol.NewShardDumpResponse(request.RequestID, shard.SelfInfo(), dump), nil
}
