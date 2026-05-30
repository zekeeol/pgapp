package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pgapp "github.com/zekee/pgapp/sdk/go/pgapp"
)

func main() {
	ctx := context.Background()
	client, err := pgapp.Dial(ctx, "127.0.0.1:50051", 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	suffix := time.Now().UnixNano()
	namespace := fmt.Sprintf("example_cache_%d", suffix)
	queue := fmt.Sprintf("example_orders_%d", suffix)

	if _, err := client.Cache().Set(ctx, namespace, "hello", []byte("world"), 60); err != nil {
		log.Fatal(err)
	}
	value, hit, err := client.Cache().Get(ctx, namespace, "hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("cache hit=%v value=%s\n", hit, string(value))

	if _, err := client.MQ().CreateQueue(ctx, queue); err != nil {
		log.Fatal(err)
	}
	messageID, err := client.MQ().SendJSON(ctx, queue, map[string]int{"order_id": 123})
	if err != nil {
		log.Fatal(err)
	}
	messages, err := client.MQ().Read(ctx, queue, 1, 30)
	if err != nil {
		log.Fatal(err)
	}
	if len(messages) == 0 || messages[0].MessageId != messageID {
		log.Fatal("message was not delivered")
	}
	if _, err := client.MQ().Ack(ctx, queue, messages[0].MessageId, messages[0].AckToken); err != nil {
		log.Fatal(err)
	}
}
