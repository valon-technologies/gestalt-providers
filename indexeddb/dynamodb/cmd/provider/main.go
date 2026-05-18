package main

import (
	"context"
	"log"

	provider "github.com/valon-technologies/gestalt-providers/indexeddb/dynamodb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func main() {
	if err := gestalt.ServeIndexedDBProvider(context.Background(), provider.New()); err != nil {
		log.Fatal(err)
	}
}
