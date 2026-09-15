package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/memberlist"
)

// NodeMeta holds metadata about a node, specifically its gRPC port.
type NodeMeta struct {
	GrpcPort int `json:"grpc_port"`
}

type Cluster struct {
	List *memberlist.Memberlist
}

// eventDelegate handles join/leave events.
type eventDelegate struct {
	onJoin  func(string)
	onLeave func(string)
}

func (ed *eventDelegate) NotifyJoin(node *memberlist.Node)   { ed.onJoin(node.Name) }
func (ed *eventDelegate) NotifyLeave(node *memberlist.Node)  { ed.onLeave(node.Name) }
func (ed *eventDelegate) NotifyUpdate(node *memberlist.Node) {}

// metaDelegate handles sharing custom metadata (like gRPC port) between nodes.
type metaDelegate struct {
	meta NodeMeta
}

func (md *metaDelegate) NodeMeta(limit int) []byte {
	b, _ := json.Marshal(md.meta)
	return b
}
func (md *metaDelegate) NotifyMsg([]byte) {}
func (md *metaDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (md *metaDelegate) LocalState(join bool) []byte                { return nil }
func (md *metaDelegate) MergeRemoteState(buf []byte, join bool)     {}

func NewCluster(nodeName string, port int, grpcPort int, joinAddr string, onJoin, onLeave func(string)) (*Cluster, error) {
	config := memberlist.DefaultLocalConfig()
	config.Name = nodeName
	config.BindPort = port
	config.AdvertisePort = port

	// To prevent logs from being too noisy in the terminal
	config.LogOutput = io.Discard

	// Setup delegates
	config.Events = &eventDelegate{onJoin: onJoin, onLeave: onLeave}
	config.Delegate = &metaDelegate{meta: NodeMeta{GrpcPort: grpcPort}}

	list, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	if joinAddr != "" {
		_, err := list.Join([]string{joinAddr})
		if err != nil {
			log.Printf("Gagal bergabung ke cluster: %v", err)
		}
	}

	return &Cluster{List: list}, nil
}

// GetNodeGrpcAddress returns the gRPC address (IP:Port) of a given node name.
func (c *Cluster) GetNodeGrpcAddress(nodeName string) (string, error) {
	for _, member := range c.List.Members() {
		if member.Name == nodeName {
			var meta NodeMeta
			if err := json.Unmarshal(member.Meta, &meta); err != nil {
				return "", fmt.Errorf("failed to parse metadata for node %s: %v", nodeName, err)
			}
			return fmt.Sprintf("%s:%d", member.Addr, meta.GrpcPort), nil
		}
	}
	return "", fmt.Errorf("node %s not found", nodeName)
}
