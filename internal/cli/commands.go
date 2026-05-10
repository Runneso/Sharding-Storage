package cli

import (
	"HM5/internal/protocol"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

type Command func(state *State, args []string, out io.Writer) error

func Commands() map[string]Command {
	return map[string]Command{
		"help":   cmdHelp,
		"router": cmdRouter,

		"addNode":     cmdAddNode,
		"removeNode":  cmdRemoveNode,
		"listNodes":   cmdListNodes,
		"setStrategy": cmdSetStrategy,
		"setRanges":   cmdSetRanges,
		"setVnodes":   cmdSetVnodes,
		"setWeight":   cmdSetWeight,
		"migrateData": cmdMigrateData,

		"put":      cmdPut,
		"get":      cmdGet,
		"delete":   cmdDelete,
		"rangeGet": cmdRangeGet,

		"dump":        cmdDump,
		"clusterDump": cmdClusterDump,
		"stats":       cmdStats,
		"ringInfo":    cmdRingInfo,
	}
}

func ExecuteLine(state *State, line string, out io.Writer) error {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}

	command, ok := Commands()[fields[0]]
	if !ok {
		return fmt.Errorf("unknown command: %s, try help", fields[0])
	}

	return command(state, fields[1:], out)
}

func cmdHelp(_ *State, _ []string, out io.Writer) error {
	_, _ = fmt.Fprintln(out, "Router:")
	_, _ = fmt.Fprintln(out, "  router <host> <port>")
	_, _ = fmt.Fprintln(out, "")
	_, _ = fmt.Fprintln(out, "Cluster commands:")
	_, _ = fmt.Fprintln(out, "  addNode <nodeId> <host> <port>")
	_, _ = fmt.Fprintln(out, "  removeNode <nodeId>")
	_, _ = fmt.Fprintln(out, "  listNodes")
	_, _ = fmt.Fprintln(out, "  setStrategy range|hash_mod_n|consistent_hashing")
	_, _ = fmt.Fprintln(out, "  setRanges <boundary1> <boundary2> ...")
	_, _ = fmt.Fprintln(out, "  setVnodes <count>")
	_, _ = fmt.Fprintln(out, "  setWeight <nodeId> <weight>")
	_, _ = fmt.Fprintln(out, "  migrateData")
	_, _ = fmt.Fprintln(out, "")
	_, _ = fmt.Fprintln(out, "User commands:")
	_, _ = fmt.Fprintln(out, "  put <key> <value>")
	_, _ = fmt.Fprintln(out, "  get <key>")
	_, _ = fmt.Fprintln(out, "  delete <key>")
	_, _ = fmt.Fprintln(out, "  rangeGet <leftKey> <rightKey>")
	_, _ = fmt.Fprintln(out, "")
	_, _ = fmt.Fprintln(out, "Debug commands:")
	_, _ = fmt.Fprintln(out, "  dump --target <nodeId>")
	_, _ = fmt.Fprintln(out, "  clusterDump")
	_, _ = fmt.Fprintln(out, "  stats")
	_, _ = fmt.Fprintln(out, "  ringInfo")
	return nil
}

func cmdRouter(state *State, args []string, out io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: router <host> <port>")
	}

	port, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("bad port: %w", err)
	}

	state.RouterHost = args[0]
	state.RouterPort = port

	_, _ = fmt.Fprintf(out, "router=%s\n", state.RouterAddr())
	return nil
}

func printClusterAck(out io.Writer, response protocol.ClusterAck) {
	_, _ = fmt.Fprintf(out, "status=%s request_id=%s", response.Status, response.RequestID)

	if response.ErrorCode != "" || response.ErrorMsg != "" {
		_, _ = fmt.Fprintf(out, " error_code=%s error_msg=%s", response.ErrorCode, response.ErrorMsg)
	}

	_, _ = fmt.Fprintln(out)
}

func printClientBase(out io.Writer, response protocol.BaseClientResponse) {
	_, _ = fmt.Fprintf(out, "status=%s node=%s request_id=%s",
		response.Status,
		response.Node.ID,
		response.RequestID,
	)

	if response.ErrorCode != "" || response.ErrorMsg != "" {
		_, _ = fmt.Fprintf(out, " error_code=%s error_msg=%s", response.ErrorCode, response.ErrorMsg)
	}

	_, _ = fmt.Fprintln(out)
}

func sortedNodeIDs(nodes map[string]protocol.NodeInfo) []string {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func sortedDumpKeys(dump protocol.Dump) []string {
	keys := make([]string, 0, len(dump))
	for key := range dump {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func normalizeStrategy(strategy string) string {
	switch strategy {
	case "range":
		return protocol.RangeBasedMode
	case "hash", "hash_mod_n":
		return protocol.HashBasedMode
	case "consistent", "consistent_hashing":
		return protocol.ConsistentHashingMode
	default:
		return strategy
	}
}

func fetchClusterInfo(state *State) (protocol.ClusterState, error) {
	request := protocol.NewClusterInfo(uuid.New())

	var response protocol.ClusterState
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return protocol.ClusterState{}, err
	}

	return response, nil
}

func fetchDump(state *State, nodeID string) (protocol.ClientDumpResponse, error) {
	request := protocol.NewClientDumpRequest(uuid.New(), nodeID)

	var response protocol.ClientDumpResponse
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return protocol.ClientDumpResponse{}, err
	}

	return response, nil
}

func cmdAddNode(state *State, args []string, out io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: addNode <nodeId> <host> <port>")
	}

	port, err := strconv.Atoi(args[2])
	if err != nil {
		return fmt.Errorf("bad port: %w", err)
	}

	request := protocol.NewClusterAddNode(uuid.New(), protocol.NodeInfo{
		ID:       args[0],
		Hostname: args[1],
		Port:     port,
	})

	var response protocol.ClusterAck
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClusterAck(out, response)
	return nil
}

func cmdRemoveNode(state *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: removeNode <nodeId>")
	}

	info, err := fetchClusterInfo(state)
	if err != nil {
		return err
	}

	node, ok := info.ClusterConfig.Nodes[args[0]]
	if !ok {
		return fmt.Errorf("unknown node: %s", args[0])
	}

	request := protocol.NewClusterRemoveNode(uuid.New(), node)

	var response protocol.ClusterAck
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClusterAck(out, response)
	return nil
}

func cmdListNodes(state *State, args []string, out io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: listNodes")
	}

	response, err := fetchClusterInfo(state)
	if err != nil {
		return err
	}

	config := response.ClusterConfig

	_, _ = fmt.Fprintf(out, "strategy=%s vnodes=%d ranges=%v\n",
		config.Strategy,
		config.VNodes,
		config.Ranges,
	)

	for _, id := range sortedNodeIDs(config.Nodes) {
		node := config.Nodes[id]
		weight := config.Weights[id]
		if weight == 0 {
			weight = 1
		}

		_, _ = fmt.Fprintf(out, "- %s %s:%d weight=%d\n",
			node.ID,
			node.Hostname,
			node.Port,
			weight,
		)
	}

	return nil
}

func cmdSetStrategy(state *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: setStrategy range|hash_mod_n|consistent_hashing")
	}

	request := protocol.NewClusterSetStrategy(uuid.New(), normalizeStrategy(args[0]))

	var response protocol.ClusterAck
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClusterAck(out, response)
	return nil
}

func cmdSetRanges(state *State, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: setRanges <boundary1> <boundary2> ...")
	}

	request := protocol.NewClusterSetRanges(uuid.New(), protocol.Boundaries(args))

	var response protocol.ClusterAck
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClusterAck(out, response)
	return nil
}

func cmdSetVnodes(state *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: setVnodes <count>")
	}

	count, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("bad count: %w", err)
	}

	request := protocol.NewClusterSetVNodes(uuid.New(), count)

	var response protocol.ClusterAck
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClusterAck(out, response)
	return nil
}

func cmdSetWeight(state *State, args []string, out io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: setWeight <nodeId> <weight>")
	}

	weight, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("bad weight: %w", err)
	}

	request := protocol.NewClusterSetWeight(uuid.New(), weight, args[0])

	var response protocol.ClusterAck
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClusterAck(out, response)
	return nil
}

func cmdMigrateData(state *State, args []string, out io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: migrateData")
	}

	request := protocol.NewClusterMigrateData(uuid.New())

	var response protocol.ClusterAck
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClusterAck(out, response)
	return nil
}
func cmdPut(state *State, args []string, out io.Writer) error {
	if len(args) < 2 {
		return errors.New("usage: put <key> <value>")
	}

	key := args[0]
	value := strings.Join(args[1:], " ")

	request := protocol.NewClientPutRequest(uuid.New(), key, value)

	var response protocol.ClientPutResponse
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClientBase(out, response.BaseClientResponse)
	return nil
}

func cmdGet(state *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: get <key>")
	}

	request := protocol.NewClientGetRequest(uuid.New(), args[0])

	var response protocol.ClientGetResponse
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClientBase(out, response.BaseClientResponse)

	if response.Status == protocol.StatusOK {
		_, _ = fmt.Fprintf(out, "found=%v", response.Found)
		if response.Found {
			_, _ = fmt.Fprintf(out, " value=%q", response.Value)
		}
		_, _ = fmt.Fprintln(out)
	}

	return nil
}

func cmdDelete(state *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: delete <key>")
	}

	request := protocol.NewClientDeleteRequest(uuid.New(), args[0])

	var response protocol.ClientDeleteResponse
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClientBase(out, response.BaseClientResponse)
	return nil
}

func cmdRangeGet(state *State, args []string, out io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: rangeGet <leftKey> <rightKey>")
	}

	request := protocol.NewClientRangeGetRequest(uuid.New(), args[0], args[1])

	var response protocol.ClientRangeGetResponse
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	printClientBase(out, response.BaseClientResponse)

	for _, key := range sortedDumpKeys(response.Dump) {
		_, _ = fmt.Fprintf(out, "%s => %q\n", key, response.Dump[key])
	}

	return nil
}
func cmdDump(state *State, args []string, out io.Writer) error {
	target := ""

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--target":
			if i+1 >= len(args) {
				return errors.New("missing --target value")
			}
			target = args[i+1]
			i++
		default:
			return fmt.Errorf("unexpected argument: %s", args[i])
		}
	}

	if target == "" {
		return errors.New("usage: dump --target <nodeId>")
	}

	response, err := fetchDump(state, target)
	if err != nil {
		return err
	}

	printClientBase(out, response.BaseClientResponse)

	_, _ = fmt.Fprintf(out, "key_count=%d\n", len(response.Dump))
	for _, key := range sortedDumpKeys(response.Dump) {
		_, _ = fmt.Fprintf(out, "%s => %q\n", key, response.Dump[key])
	}

	return nil
}

func cmdClusterDump(state *State, args []string, out io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: clusterDump")
	}

	info, err := fetchClusterInfo(state)
	if err != nil {
		return err
	}

	for _, id := range sortedNodeIDs(info.ClusterConfig.Nodes) {
		response, err := fetchDump(state, id)
		if err != nil {
			_, _ = fmt.Fprintf(out, "node=%s error=%v\n", id, err)
			continue
		}

		_, _ = fmt.Fprintf(out, "node=%s status=%s key_count=%d\n",
			id,
			response.Status,
			len(response.Dump),
		)

		for _, key := range sortedDumpKeys(response.Dump) {
			_, _ = fmt.Fprintf(out, "  %s => %q\n", key, response.Dump[key])
		}
	}

	return nil
}

func cmdStats(state *State, args []string, out io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: stats")
	}

	info, err := fetchClusterInfo(state)
	if err != nil {
		return err
	}

	ids := sortedNodeIDs(info.ClusterConfig.Nodes)
	if len(ids) == 0 {
		return errors.New("cluster is empty")
	}

	counts := make(map[string]int, len(ids))
	total := 0
	maxCount := 0

	for _, id := range ids {
		response, err := fetchDump(state, id)
		if err != nil {
			_, _ = fmt.Fprintf(out, "node=%s error=%v\n", id, err)
			continue
		}

		count := len(response.Dump)
		counts[id] = count
		total += count

		if count > maxCount {
			maxCount = count
		}
	}

	avg := float64(total) / float64(len(ids))

	var variance float64
	for _, id := range ids {
		diff := float64(counts[id]) - avg
		variance += diff * diff
	}
	variance /= float64(len(ids))

	stddev := math.Sqrt(variance)

	stddevMean := 0.0
	maxAvg := 0.0

	if avg > 0 {
		stddevMean = stddev / avg
		maxAvg = float64(maxCount) / avg
	}

	_, _ = fmt.Fprintf(out, "total_keys=%d nodes=%d avg=%.2f stddev=%.2f stddev/mean=%.4f max/avg=%.4f\n",
		total,
		len(ids),
		avg,
		stddev,
		stddevMean,
		maxAvg,
	)

	for _, id := range ids {
		_, _ = fmt.Fprintf(out, "- %s keys=%d\n", id, counts[id])
	}

	return nil
}

func cmdRingInfo(state *State, args []string, out io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: ringInfo")
	}

	request := protocol.NewRingInfoRequest(uuid.New())

	var response protocol.RingInfoResponse
	if err := sendJSONLine(state.RouterAddr(), request, &response); err != nil {
		return err
	}

	sort.Slice(response.Ring, func(i, j int) bool {
		if response.Ring[i].Hash == response.Ring[j].Hash {
			return response.Ring[i].Node.ID < response.Ring[j].Node.ID
		}
		return response.Ring[i].Hash < response.Ring[j].Hash
	})

	_, _ = fmt.Fprintf(out, "ring_size=%d\n", len(response.Ring))
	for _, item := range response.Ring {
		_, _ = fmt.Fprintf(out, "%020d -> %s\n", item.Hash, item.Node.ID)
	}

	return nil
}
