package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/zekee/pgapp/sdk/go/gen/pgapp/v1"
	pgapp "github.com/zekee/pgapp/sdk/go/pgapp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestLiveGoSDKCacheAndMQ(t *testing.T) {
	endpoint := liveEndpoint(t)
	client, err := pgapp.Dial(context.Background(), endpoint, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	suffix := time.Now().UnixNano()
	namespace := fmt.Sprintf("go_sdk_cache_%d", suffix)
	queue := fmt.Sprintf("go_sdk_orders_%d", suffix)

	ok, err := client.Cache().Set(context.Background(), namespace, "hello", []byte("world"), 60)
	if err != nil || !ok {
		t.Fatalf("cache set failed: ok=%v err=%v", ok, err)
	}
	value, hit, err := client.Cache().Get(context.Background(), namespace, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if !hit || string(value) != "world" {
		t.Fatalf("unexpected cache response hit=%v value=%q", hit, value)
	}

	ok, err = client.MQ().CreateQueue(context.Background(), queue)
	if err != nil || !ok {
		t.Fatalf("create queue failed: ok=%v err=%v", ok, err)
	}
	messageID, err := client.MQ().SendJSON(context.Background(), queue, map[string]bool{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := client.MQ().Read(context.Background(), queue, 1, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].MessageId != messageID {
		t.Fatalf("unexpected messages: %#v", messages)
	}
	if messages[0].AckToken == "" {
		t.Fatal("expected ack token")
	}
	ok, err = client.MQ().Ack(context.Background(), queue, messageID, messages[0].AckToken)
	if err != nil || !ok {
		t.Fatalf("ack failed: ok=%v err=%v", ok, err)
	}
}

func TestLiveGoSDKExposesPhaseOneSurface(t *testing.T) {
	endpoint := liveEndpoint(t)
	client, err := pgapp.Dial(context.Background(), endpoint, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	suffix := time.Now().UnixNano()
	namespace := fmt.Sprintf("go_sdk_surface_cache_%d", suffix)
	queue := fmt.Sprintf("go_sdk_surface_orders_%d", suffix)

	ok, err := client.Cache().Set(context.Background(), namespace, "a", []byte("one"), 60)
	if err != nil || !ok {
		t.Fatalf("cache set failed: ok=%v err=%v", ok, err)
	}
	ok, err = client.Cache().Set(context.Background(), namespace, "b", []byte("two"), 60)
	if err != nil || !ok {
		t.Fatalf("cache set failed: ok=%v err=%v", ok, err)
	}
	items, err := client.Cache().MGet(context.Background(), namespace, []string{"a", "missing"})
	if err != nil || len(items) != 2 {
		t.Fatalf("unexpected mget: items=%#v err=%v", items, err)
	}
	exists, err := client.Cache().Exists(context.Background(), namespace, "a")
	if err != nil || !exists {
		t.Fatalf("exists failed: exists=%v err=%v", exists, err)
	}
	ok, err = client.Cache().Delete(context.Background(), namespace, "b")
	if err != nil || !ok {
		t.Fatalf("delete failed: ok=%v err=%v", ok, err)
	}
	ok, err = client.Cache().InvalidateNamespace(context.Background(), namespace)
	if err != nil || !ok {
		t.Fatalf("invalidate namespace failed: ok=%v err=%v", ok, err)
	}
	cacheStats, err := client.Cache().Stats(context.Background())
	if err != nil || cacheStats.Writes < 2 {
		t.Fatalf("unexpected cache stats: stats=%#v err=%v", cacheStats, err)
	}

	ok, err = client.MQ().CreateQueue(context.Background(), queue)
	if err != nil || !ok {
		t.Fatalf("create queue failed: ok=%v err=%v", ok, err)
	}
	ids, err := client.MQ().SendBatchJSON(context.Background(), queue, []interface{}{
		map[string]int{"n": 1},
		map[string]int{"n": 2},
	}, 0)
	if err != nil || len(ids) != 2 {
		t.Fatalf("unexpected send batch: ids=%#v err=%v", ids, err)
	}
	messages, err := client.MQ().ReadWithPoll(context.Background(), queue, 1, 30, 1, 25)
	if err != nil || len(messages) != 1 {
		t.Fatalf("unexpected long poll: messages=%#v err=%v", messages, err)
	}
	ok, err = client.MQ().SetVisibilityTimeout(context.Background(), queue, messages[0].MessageId, messages[0].AckToken, 30)
	if err != nil || !ok {
		t.Fatalf("set vt failed: ok=%v err=%v", ok, err)
	}
	ok, err = client.MQ().Archive(context.Background(), queue, messages[0].MessageId, messages[0].AckToken)
	if err != nil || !ok {
		t.Fatalf("archive failed: ok=%v err=%v", ok, err)
	}
	mqStats, err := client.MQ().Metrics(context.Background(), queue)
	if err != nil || mqStats.ArchivedMessageCount != 1 {
		t.Fatalf("unexpected mq metrics: metrics=%#v err=%v", mqStats, err)
	}
	ok, err = client.MQ().PurgeQueue(context.Background(), queue)
	if err != nil || !ok {
		t.Fatalf("purge failed: ok=%v err=%v", ok, err)
	}
	ok, err = client.MQ().DropQueue(context.Background(), queue)
	if err != nil || !ok {
		t.Fatalf("drop failed: ok=%v err=%v", ok, err)
	}
}

func TestLiveGoSDKConfigReadAndWatch(t *testing.T) {
	endpoint := liveEndpoint(t)
	client, err := pgapp.Dial(context.Background(), endpoint, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	scope := pgapp.NewConfigScope(
		fmt.Sprintf("go_sdk_config_%d", time.Now().UnixNano()),
		"prod",
		"default",
		"application",
	)
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	generated := pb.NewConfigServiceClient(conn)
	if _, err := generated.UpsertItem(context.Background(), &pb.UpsertConfigItemRequest{
		Scope:     scope,
		Key:       "feature_flags",
		JsonValue: `{"enabled":true}`,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := generated.Publish(context.Background(), &pb.PublishConfigRequest{
		Scope:       scope,
		Message:     "sdk release",
		PublishedBy: "go-sdk-test",
	}); err != nil {
		t.Fatal(err)
	}

	release, err := client.Config().GetLatestRelease(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if release.Revision != 1 || release.Snapshot["feature_flags"] == nil {
		t.Fatalf("unexpected config release: %#v", release)
	}
	watch, err := client.Config().Watch(context.Background(), scope, release.Revision, 0)
	if err != nil {
		t.Fatal(err)
	}
	if watch.Changed || watch.LatestRevision != release.Revision || watch.Release != nil {
		t.Fatalf("unexpected watch no-change result: %#v", watch)
	}
}

func TestLiveGoSDKPreservesErrorStatus(t *testing.T) {
	endpoint := liveEndpoint(t)
	client, err := pgapp.Dial(context.Background(), endpoint, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Cache().Set(context.Background(), "bad namespace", "key", []byte("value"), 60)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (%v)", status.Code(err), err)
	}
}

func TestLiveGoSDKPhaseTwoCacheMQAndStreamSurface(t *testing.T) {
	endpoint := liveEndpoint(t)
	client, err := pgapp.Dial(context.Background(), endpoint, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	suffix := time.Now().UnixNano()
	namespace := fmt.Sprintf("go_sdk_phase_two_cache_%d", suffix)
	queue := fmt.Sprintf("go_sdk_phase_two_orders_%d", suffix)

	value, err := client.Cache().Increment(context.Background(), namespace, "counter", 2, 60)
	if err != nil || value != 2 {
		t.Fatalf("increment failed: value=%d err=%v", value, err)
	}
	value, err = client.Cache().Decrement(context.Background(), namespace, "counter", 1, 0)
	if err != nil || value != 1 {
		t.Fatalf("decrement failed: value=%d err=%v", value, err)
	}
	created, err := client.Cache().SetNX(context.Background(), namespace, "lock", []byte("first"), 60)
	if err != nil || !created {
		t.Fatalf("set nx failed: created=%v err=%v", created, err)
	}
	created, err = client.Cache().SetNX(context.Background(), namespace, "lock", []byte("second"), 60)
	if err != nil || created {
		t.Fatalf("set nx existing failed: created=%v err=%v", created, err)
	}
	old, hit, err := client.Cache().GetSet(context.Background(), namespace, "slot", []byte("new"), 60)
	if err != nil || hit || old != nil {
		t.Fatalf("unexpected first getset: old=%q hit=%v err=%v", old, hit, err)
	}
	old, hit, err = client.Cache().GetSet(context.Background(), namespace, "slot", []byte("newer"), 0)
	if err != nil || !hit || string(old) != "new" {
		t.Fatalf("unexpected second getset: old=%q hit=%v err=%v", old, hit, err)
	}
	length, err := client.Cache().Append(context.Background(), namespace, "log", []byte("tail"), 0)
	if err != nil || length != 4 {
		t.Fatalf("append failed: length=%d err=%v", length, err)
	}
	length, err = client.Cache().Prepend(context.Background(), namespace, "log", []byte("head-"), 0)
	if err != nil || length != 9 {
		t.Fatalf("prepend failed: length=%d err=%v", length, err)
	}

	ok, err := client.MQ().CreateQueue(context.Background(), queue)
	if err != nil || !ok {
		t.Fatalf("create queue failed: ok=%v err=%v", ok, err)
	}
	dlq, err := client.MQ().ListDLQMessages(context.Background(), queue, 10, 0)
	if err != nil || len(dlq) != 0 {
		t.Fatalf("unexpected empty dlq: %#v err=%v", dlq, err)
	}
	stream, err := client.MQ().StreamRead(context.Background(), queue, 1, 30)
	if err != nil {
		t.Fatal(err)
	}
	messageID, err := client.MQ().SendJSON(context.Background(), queue, map[string]bool{"stream": true})
	if err != nil {
		t.Fatal(err)
	}
	read, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Messages) != 1 || read.Messages[0].MessageId != messageID {
		t.Fatalf("unexpected stream response: %#v", read.Messages)
	}
}

func liveEndpoint(t *testing.T) string {
	t.Helper()
	endpoint := getenv("PGAPP_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("PGAPP_TEST_ENDPOINT is not set")
	}
	return endpoint
}

func getenv(key string) string {
	return envLookup(key)
}
