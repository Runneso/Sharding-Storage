package protocol

import (
	"errors"

	"github.com/google/uuid"
)

const (
	// Client -> Router
	TypeClientPutRequest      = "CLIENT_PUT_REQUEST"
	TypeClientDeleteRequest   = "CLIENT_DELETE_REQUEST"
	TypeClientGetRequest      = "CLIENT_GET_REQUEST"
	TypeClientDumpRequest     = "CLIENT_DUMP_REQUEST"
	TypeClientRangeGetRequest = "CLIENT_RANGE_GET_REQUEST"

	// Router -> Client
	TypeClientPutResponse      = "CLIENT_PUT_RESPONSE"
	TypeClientDeleteResponse   = "CLIENT_DELETE_RESPONSE"
	TypeClientGetResponse      = "CLIENT_GET_RESPONSE"
	TypeClientDumpResponse     = "CLIENT_DUMP_RESPONSE"
	TypeClientRangeGetResponse = "CLIENT_RANGE_GET_RESPONSE"

	// Router -> Shard
	TypeShardPutRequest    = "SHARD_PUT_REQUEST"
	TypeShardDeleteRequest = "SHARD_DELETE_REQUEST"
	TypeShardGetRequest    = "SHARD_GET_REQUEST"
	TypeShardDumpRequest   = "SHARD_DUMP_REQUEST"

	// Shard -> Router
	TypeShardPutResponse    = "SHARD_PUT_RESPONSE"
	TypeShardDeleteResponse = "SHARD_DELETE_RESPONSE"
	TypeShardGetResponse    = "SHARD_GET_RESPONSE"
	TypeShardDumpResponse   = "SHARD_DUMP_RESPONSE"

	// Client -> Router
	TypeClusterAddNode     = "CLUSTER_ADD_NODE"
	TypeClusterRemoveNode  = "CLUSTER_REMOVE_NODE"
	TypeClusterSetStrategy = "CLUSTER_SET_STRATEGY"
	TypeClusterSetVNodes   = "CLUSTER_SET_VNODES"
	TypeClusterSetRanges   = "CLUSTER_SET_RANGES"
	TypeClusterSetWeight   = "CLUSTER_SET_WEIGHT"
	TypeClusterMigrateData = "CLUSTER_MIGRATE_DATA"
	TypeClusterInfo        = "CLUSTER_INFO"
	TypeRingInfoRequest    = "RING_INFO_REQUEST"

	// Router -> Client
	TypeClusterAck       = "CLUSTER_ACK"
	TypeClusterState     = "CLUSTER_STATE"
	TypeRingInfoResponse = "RING_INFO_RESPONSE"
)

type BaseMessage struct {
	RequestID uuid.UUID `json:"request_id"`
	Type      string    `json:"type"`
}

type ClientPutRequest struct {
	BaseMessage
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewClientPutRequest(requestID uuid.UUID, key, value string) *ClientPutRequest {
	return &ClientPutRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClientPutRequest,
		},
		Key:   key,
		Value: value,
	}
}

type ClientDeleteRequest struct {
	BaseMessage
	Key string `json:"key"`
}

func NewClientDeleteRequest(requestID uuid.UUID, key string) *ClientDeleteRequest {
	return &ClientDeleteRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClientDeleteRequest,
		},
		Key: key,
	}
}

type ClientGetRequest struct {
	BaseMessage
	Key string `json:"key"`
}

func NewClientGetRequest(requestID uuid.UUID, key string) *ClientGetRequest {
	return &ClientGetRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClientGetRequest,
		},
		Key: key,
	}
}

type ClientDumpRequest struct {
	BaseMessage
	NodeID string `json:"node_id"`
}

func NewClientDumpRequest(requestID uuid.UUID, nodeID string) *ClientDumpRequest {
	return &ClientDumpRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClientDumpRequest,
		},
		NodeID: nodeID,
	}
}

type ClientRangeGetRequest struct {
	BaseMessage
	LeftKey  string `json:"left_key"`
	RightKey string `json:"right_key"`
}

func NewClientRangeGetRequest(requestID uuid.UUID, leftKey, rightKey string) *ClientRangeGetRequest {
	return &ClientRangeGetRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClientRangeGetRequest,
		},
		LeftKey:  leftKey,
		RightKey: rightKey,
	}
}

type BaseClientResponse struct {
	BaseMessage
	Node      NodeInfo `json:"node"`
	Status    string   `json:"status"` // "OK" or "ERROR"
	ErrorCode string   `json:"error_code,omitempty"`
	ErrorMsg  string   `json:"error_msg,omitempty"`
}

func NewBaseClientResponse(requestID uuid.UUID, responseType string, node NodeInfo, err error) *BaseClientResponse {
	if err == nil {
		return &BaseClientResponse{
			BaseMessage: BaseMessage{
				RequestID: requestID,
				Type:      responseType,
			},
			Node:   node,
			Status: StatusOK,
		}
	}

	if ae, ok := errors.AsType[ApplicationError](err); ok {
		return &BaseClientResponse{
			BaseMessage: BaseMessage{
				RequestID: requestID,
				Type:      responseType,
			},
			Node:      node,
			Status:    StatusError,
			ErrorCode: ae.ErrorCode(),
			ErrorMsg:  ae.Error(),
		}
	}

	return &BaseClientResponse{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      responseType,
		},
		Node:      node,
		Status:    StatusError,
		ErrorCode: ErrorBadRequest,
		ErrorMsg:  err.Error(),
	}
}

type ClientPutResponse struct {
	BaseClientResponse
}

func NewClientPutResponse(requestID uuid.UUID, node NodeInfo, error error) *ClientPutResponse {
	return &ClientPutResponse{
		BaseClientResponse: *NewBaseClientResponse(requestID, TypeClientPutResponse, node, error),
	}
}

type ClientDeleteResponse struct {
	BaseClientResponse
}

func NewClientDeleteResponse(requestID uuid.UUID, node NodeInfo, error error) *ClientDeleteResponse {
	return &ClientDeleteResponse{
		BaseClientResponse: *NewBaseClientResponse(requestID, TypeClientDeleteResponse, node, error),
	}
}

type ClientGetResponse struct {
	BaseClientResponse
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
}

func NewClientGetResponse(requestID uuid.UUID, node NodeInfo, error error, value string, found bool) *ClientGetResponse {
	return &ClientGetResponse{
		BaseClientResponse: *NewBaseClientResponse(requestID, TypeClientGetResponse, node, error),
		Found:              found,
		Value:              value,
	}
}

type ClientDumpResponse struct {
	BaseClientResponse
	Dump Dump `json:"dump"`
}

func NewClientDumpResponse(requestID uuid.UUID, node NodeInfo, error error, dump Dump) *ClientDumpResponse {
	return &ClientDumpResponse{
		BaseClientResponse: *NewBaseClientResponse(requestID, TypeClientDumpResponse, node, error),
		Dump:               dump,
	}
}

type ClientRangeGetResponse struct {
	BaseClientResponse
	Dump Dump `json:"dump"`
}

func NewClientRangeGetResponse(requestID uuid.UUID, node NodeInfo, error error, dump Dump) *ClientRangeGetResponse {
	return &ClientRangeGetResponse{
		BaseClientResponse: *NewBaseClientResponse(requestID, TypeClientRangeGetResponse, node, error),
		Dump:               dump,
	}
}

type ShardPutRequest struct {
	BaseMessage
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewShardPutRequest(requestID uuid.UUID, key string, value string) *ShardPutRequest {
	return &ShardPutRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeShardPutRequest,
		},
		Key:   key,
		Value: value,
	}
}

type ShardDeleteRequest struct {
	BaseMessage
	Key string `json:"key"`
}

func NewShardDeleteRequest(requestID uuid.UUID, key string) *ShardDeleteRequest {
	return &ShardDeleteRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeShardDeleteRequest,
		},
		Key: key,
	}
}

type ShardGetRequest struct {
	BaseMessage
	Key string `json:"key"`
}

func NewShardGetRequest(requestID uuid.UUID, key string) *ShardGetRequest {
	return &ShardGetRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeShardGetRequest,
		},
		Key: key,
	}
}

type ShardDumpRequest struct {
	BaseMessage
}

func NewShardDumpRequest(requestID uuid.UUID) *ShardDumpRequest {
	return &ShardDumpRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeShardDumpRequest,
		},
	}
}

type BaseShardResponse struct {
	BaseMessage
	Node NodeInfo `json:"node"`
}

func NewBaseShardResponse(requestID uuid.UUID, responseType string, node NodeInfo) *BaseShardResponse {
	return &BaseShardResponse{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      responseType,
		},
		Node: node,
	}
}

type ShardPutResponse struct {
	BaseShardResponse
}

func NewShardPutResponse(requestID uuid.UUID, node NodeInfo) *ShardPutResponse {
	return &ShardPutResponse{
		BaseShardResponse: *NewBaseShardResponse(requestID, TypeShardPutResponse, node),
	}
}

type ShardDeleteResponse struct {
	BaseShardResponse
}

func NewShardDeleteResponse(requestID uuid.UUID, nodeInfo NodeInfo) *ShardDeleteResponse {
	return &ShardDeleteResponse{
		BaseShardResponse: *NewBaseShardResponse(requestID, TypeShardDeleteResponse, nodeInfo),
	}
}

type ShardGetResponse struct {
	BaseShardResponse
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
}

func NewShardGetResponse(requestID uuid.UUID, value string, found bool, node NodeInfo) *ShardGetResponse {
	return &ShardGetResponse{
		BaseShardResponse: *NewBaseShardResponse(requestID, TypeShardGetResponse, node),
		Value:             value,
		Found:             found,
	}
}

type ShardDumpResponse struct {
	BaseShardResponse
	Dump Dump `json:"dump"`
}

func NewShardDumpResponse(requestID uuid.UUID, node NodeInfo, dump Dump) *ShardDumpResponse {
	return &ShardDumpResponse{
		BaseShardResponse: *NewBaseShardResponse(requestID, TypeShardDumpResponse, node),
		Dump:              dump,
	}
}

type ClusterAddNode struct {
	BaseMessage
	Node NodeInfo `json:"node"`
}

func NewClusterAddNode(requestID uuid.UUID, node NodeInfo) *ClusterAddNode {
	return &ClusterAddNode{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterAddNode,
		},
		Node: node,
	}
}

type ClusterRemoveNode struct {
	BaseMessage
	Node NodeInfo `json:"node"`
}

func NewClusterRemoveNode(requestID uuid.UUID, node NodeInfo) *ClusterRemoveNode {
	return &ClusterRemoveNode{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterRemoveNode,
		},
		Node: node,
	}
}

type ClusterSetStrategy struct {
	BaseMessage
	Strategy string `json:"strategy"` // "range" or "hash" or "consistent"
}

func NewClusterSetStrategy(requestID uuid.UUID, strategy string) *ClusterSetStrategy {
	return &ClusterSetStrategy{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterSetStrategy,
		},
		Strategy: strategy,
	}
}

type ClusterSetVNodes struct {
	BaseMessage
	Count int `json:"count"`
}

func NewClusterSetVNodes(requestID uuid.UUID, count int) *ClusterSetVNodes {
	return &ClusterSetVNodes{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterSetVNodes,
		},
		Count: count,
	}
}

type ClusterSetRanges struct {
	BaseMessage
	Boundaries Boundaries `json:"boundaries"`
}

func NewClusterSetRanges(requestID uuid.UUID, boundaries Boundaries) *ClusterSetRanges {
	return &ClusterSetRanges{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterSetRanges,
		},
		Boundaries: boundaries,
	}
}

type ClusterSetWeight struct {
	BaseMessage
	NodeID string `json:"node_id"`
	Weight int    `json:"weight"`
}

func NewClusterSetWeight(requestID uuid.UUID, weight int, nodeID string) *ClusterSetWeight {
	return &ClusterSetWeight{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterSetWeight,
		},
		NodeID: nodeID,
		Weight: weight,
	}
}

type ClusterMigrateData struct {
	BaseMessage
}

func NewClusterMigrateData(requestID uuid.UUID) *ClusterMigrateData {
	return &ClusterMigrateData{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterMigrateData,
		},
	}
}

type ClusterAck struct {
	BaseMessage
	Status    string `json:"status"` // "OK" or "ERROR"
	ErrorCode string `json:"error_code,omitempty"`
	ErrorMsg  string `json:"error_message,omitempty"`
}

func NewClusterAck(requestID uuid.UUID, err error) *ClusterAck {
	if err == nil {
		return &ClusterAck{
			BaseMessage: BaseMessage{
				RequestID: requestID,
				Type:      TypeClusterAck,
			},
			Status: StatusOK,
		}
	}

	if ae, ok := errors.AsType[ApplicationError](err); ok {
		return &ClusterAck{
			BaseMessage: BaseMessage{
				RequestID: requestID,
				Type:      TypeClusterAck,
			},
			Status:    StatusError,
			ErrorCode: ae.ErrorCode(),
			ErrorMsg:  ae.Error(),
		}
	}

	return &ClusterAck{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterAck,
		},
		Status:    StatusError,
		ErrorCode: ErrorInvalidClusterConfig,
		ErrorMsg:  err.Error(),
	}
}

type ClusterInfo struct {
	BaseMessage
}

func NewClusterInfo(requestID uuid.UUID) *ClusterInfo {
	return &ClusterInfo{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterInfo,
		},
	}
}

type ClusterState struct {
	BaseMessage
	ClusterConfig ClusterConfig `json:"cluster_config"`
}

func NewClusterState(requestID uuid.UUID, config ClusterConfig) *ClusterState {
	return &ClusterState{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeClusterState,
		},
		ClusterConfig: config,
	}
}

type RingInfoRequest struct {
	BaseMessage
}

func NewRingInfoRequest(requestID uuid.UUID) *RingInfoRequest {
	return &RingInfoRequest{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeRingInfoRequest,
		},
	}
}

type RingInfoResponse struct {
	BaseMessage
	Ring []RingNode `json:"ring"`
}

func NewRingInfoResponse(requestID uuid.UUID, ring []RingNode) *RingInfoResponse {
	return &RingInfoResponse{
		BaseMessage: BaseMessage{
			RequestID: requestID,
			Type:      TypeRingInfoResponse,
		},
		Ring: ring,
	}
}
