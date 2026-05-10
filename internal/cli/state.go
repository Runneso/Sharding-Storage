package cli

import (
	"HM5/internal/protocol"
	"net"
	"strconv"
)

type State struct {
	RouterHost string
	RouterPort int
}

func NewState() *State {
	return &State{
		RouterHost: "127.0.0.1",
		RouterPort: 8081,
	}
}

func (state *State) RouterAddr() string {
	return net.JoinHostPort(state.RouterHost, strconv.Itoa(state.RouterPort))
}

func addrOfNode(node protocol.NodeInfo) string {
	return net.JoinHostPort(node.Hostname, strconv.Itoa(node.Port))
}
