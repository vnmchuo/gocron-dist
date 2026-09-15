package hash

import (
	"context"
	"hash/crc32"
	"sort"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	go_trace "go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("consistent-hash")

type Consistent struct {
	nodes   []uint32          // Stores sorted hash of each node
	nodeMap map[uint32]string // Map from hash to original node name (e.g., "node-1")
	mu      sync.RWMutex
}

func NewConsistent() *Consistent {
	return &Consistent{
		nodeMap: make(map[uint32]string),
	}
}

// AddNode adds a new node to the ring
func (c *Consistent) AddNode(nodeName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := crc32.ChecksumIEEE([]byte(nodeName))
	if _, exists := c.nodeMap[hash]; exists {
		return
	}
	c.nodes = append(c.nodes, hash)
	c.nodeMap[hash] = nodeName
	sort.Slice(c.nodes, func(i, j int) bool { return c.nodes[i] < c.nodes[j] })
}

// RemoveNode removes a node from the ring
func (c *Consistent) RemoveNode(nodeName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := crc32.ChecksumIEEE([]byte(nodeName))
	if _, exists := c.nodeMap[hash]; !exists {
		return
	}

	delete(c.nodeMap, hash)

	// Remove from sorted nodes
	for i, v := range c.nodes {
		if v == hash {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			break
		}
	}
}

// GetNode finds which node is responsible for a given ID (Job ID)
func (c *Consistent) GetNode(key string) string {
	return c.GetNodeWithContext(context.Background(), key)
}

func (c *Consistent) GetNodeWithContext(ctx context.Context, key string) string {
	_, span := tracer.Start(ctx, "GetNode", go_trace.WithAttributes(
		attribute.String("lookup_key", key),
	))
	defer span.End()

	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.nodes) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// Find the first node whose hash is >= key hash
	idx := sort.Search(len(c.nodes), func(i int) bool {
		return c.nodes[i] >= hash
	})

	// If no larger node found, wrap around to the first node (circular)
	if idx == len(c.nodes) {
		idx = 0
	}

	owner := c.nodeMap[c.nodes[idx]]
	span.SetAttributes(attribute.String("owner", owner))
	return owner
}
