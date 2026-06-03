package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	pgapp "github.com/zekee/pgapp/sdk/go/pgapp"
)

func TestClientKeepsEndpointAndTimeout(t *testing.T) {
	client := pgapp.NewClient("http://127.0.0.1:50051", 2*time.Second)
	if client.Endpoint() != "http://127.0.0.1:50051" {
		t.Fatalf("unexpected endpoint: %s", client.Endpoint())
	}
	if client.Timeout() != 2*time.Second {
		t.Fatalf("unexpected timeout: %s", client.Timeout())
	}
	if client.Cache() == nil || client.MQ() == nil {
		t.Fatal("expected cache and mq clients")
	}
	if client.Config() == nil {
		t.Fatal("expected config client")
	}
}

func TestClientReportsNotConnectedForConfigOnlyClient(t *testing.T) {
	client := pgapp.NewClient("127.0.0.1:50051", time.Second)
	_, err := client.MQ().SendJSON(context.Background(), "orders", map[string]bool{"ok": true})
	if !errors.Is(err, pgapp.ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}

func TestDialCreatesGeneratedGrpcWrappers(t *testing.T) {
	client, err := pgapp.Dial(context.Background(), "127.0.0.1:1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if client.Endpoint() != "127.0.0.1:1" {
		t.Fatalf("unexpected endpoint: %s", client.Endpoint())
	}
	if client.Cache() == nil || client.MQ() == nil || client.Config() == nil {
		t.Fatal("expected cache, mq, and config wrappers")
	}
}

func TestPhaseTwoAPISurfaceCompiles(t *testing.T) {
	client := pgapp.NewClient("127.0.0.1:50051", time.Second)
	ctx := context.Background()
	_, _ = client.Cache().Increment(ctx, "ns", "counter", 1, 60)
	_, _ = client.Cache().Decrement(ctx, "ns", "counter", 1, 0)
	_, _ = client.Cache().SetNX(ctx, "ns", "lock", []byte("1"), 60)
	_, _, _ = client.Cache().GetSet(ctx, "ns", "slot", []byte("v2"), 0)
	_, _ = client.Cache().Append(ctx, "ns", "log", []byte("tail"), 0)
	_, _ = client.Cache().Prepend(ctx, "ns", "log", []byte("head"), 0)
	_, _ = client.MQ().ListDLQMessages(ctx, "orders", 10, 0)
	_, _ = client.MQ().GetDLQMessage(ctx, "orders", 1)
	_, _ = client.MQ().ReprocessDLQMessage(ctx, "orders", 1)
	_, _ = client.MQ().PurgeDLQ(ctx, "orders")
	_, _ = client.MQ().StreamRead(ctx, "orders", 1, 30)
	authed := pgapp.NewClientWithCredentials("127.0.0.1:50051", time.Second, "key", "secret")
	if authed == nil {
		t.Fatal("expected client")
	}
}
