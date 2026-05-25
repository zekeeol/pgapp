package tests

import (
	"testing"

	pb "github.com/zekee/pgapp/sdk/go/gen/pgapp/v1"
)

func TestGeneratedGoClientTypesExist(t *testing.T) {
	req := &pb.SetCacheRequest{Namespace: "default", Key: "hello", Value: []byte("world")}
	if string(req.Value) != "world" {
		t.Fatalf("unexpected value: %q", req.Value)
	}
	if pb.QueueStorageMode_QUEUE_STORAGE_MODE_DURABLE.Number() == 0 {
		t.Fatal("expected durable queue enum")
	}
}
