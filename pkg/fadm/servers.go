package fadm

import (
	"context"
	"fmt"
	"sort"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
)

// ServerNode describes a coordinator or tablet server advertised by cluster metadata.
type ServerNode struct {
	// ID is the server node identifier.
	ID int32
	// Host is the advertised hostname or address.
	Host string
	// Port is the advertised service port.
	Port int32
	// Role is the Fluss coordinator or tablet role.
	Role fgo.ServerRole
	// Rack is optional placement metadata.
	Rack string
}

// ServerNodes returns the current coordinator followed by the alive tablet servers.
// Tablet servers are sorted by node ID, host, and port for deterministic results.
func (c *Client) ServerNodes(ctx context.Context) ([]ServerNode, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyGetMetadata, 0)
	if err != nil {
		return nil, err
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	metadata, ok := response.Message().(*fmsg.MetadataResponse)
	if !ok {
		return nil, unexpected("server nodes", response)
	}
	coordinator, err := serverNode(metadata.GetCoordinatorServer(), fgo.Coordinator)
	if err != nil {
		return nil, err
	}
	tablets := make([]ServerNode, len(metadata.GetTabletServers()))
	for index, advertised := range metadata.GetTabletServers() {
		tablets[index], err = serverNode(advertised, fgo.TabletServer)
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(tablets, func(left, right int) bool {
		if tablets[left].ID != tablets[right].ID {
			return tablets[left].ID < tablets[right].ID
		}
		if tablets[left].Host != tablets[right].Host {
			return tablets[left].Host < tablets[right].Host
		}
		return tablets[left].Port < tablets[right].Port
	})
	return append([]ServerNode{coordinator}, tablets...), nil
}

func serverNode(node *fmsg.PbServerNode, role fgo.ServerRole) (ServerNode, error) {
	if node == nil || node.GetHost() == "" || node.GetPort() <= 0 || node.GetPort() > 65535 {
		return ServerNode{}, fmt.Errorf("%w: invalid %v server node", fgo.ErrMetadata, role)
	}
	return ServerNode{
		ID: node.GetNodeId(), Host: node.GetHost(), Port: node.GetPort(), Role: role, Rack: node.GetRack(),
	}, nil
}
