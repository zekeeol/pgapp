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
	message := &pb.QueueMessage{MessageId: 42, AckToken: "receipt"}
	ack := &pb.AckMessageRequest{QueueName: "orders", MessageId: message.MessageId, AckToken: message.AckToken}
	if ack.GetAckToken() != "receipt" {
		t.Fatalf("unexpected ack token: %q", ack.GetAckToken())
	}
	scope := &pb.ConfigScope{AppId: "billing", Environment: "prod", Cluster: "default", Namespace: "application"}
	watch := &pb.WatchConfigRequest{Scope: scope, KnownRevision: 1, TimeoutSeconds: 30}
	if watch.Scope.GetAppId() != "billing" {
		t.Fatalf("unexpected config scope: %#v", watch.Scope)
	}
}
