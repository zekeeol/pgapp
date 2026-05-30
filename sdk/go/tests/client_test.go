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
