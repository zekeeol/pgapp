package main

import (
	"fmt"
	"time"

	pgapp "github.com/zekee/pgapp/sdk/go/pgapp"
)

func main() {
	client := pgapp.NewClient("http://127.0.0.1:50051", 3*time.Second)
	fmt.Println(client.Endpoint())
}
