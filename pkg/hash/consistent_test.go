package hash_test

import (
	"testing"

	"github.com/vnmchuo/gocron-dist/pkg/hash"
)

func TestConsistent_AddNode(t *testing.T) {
	c := hash.NewConsistent()
	c.AddNode("node-1")
	c.AddNode("node-2")

	node := c.GetNode("some-job-key")
	if node == "" {
		t.Error("GetNode should return a node name")
	}
	if node != "node-1" && node != "node-2" {
		t.Errorf("GetNode returned unexpected node: %s", node)
	}
}

func TestConsistent_Consistency(t *testing.T) {
	c := hash.NewConsistent()
	c.AddNode("node-A")
	c.AddNode("node-B")
	c.AddNode("node-C")

	key := "job-123"
	node1 := c.GetNode(key)

	// Repeating the request should return the same node
	node2 := c.GetNode(key)
	if node1 != node2 {
		t.Errorf("Consistent hashing failed: got %s then %s", node1, node2)
	}
}

func TestConsistent_Empty(t *testing.T) {
	c := hash.NewConsistent()
	node := c.GetNode("any-key")
	if node != "" {
		t.Error("GetNode on empty ring should return empty string")
	}
}
