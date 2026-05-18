package main

import (
	"context"
	"log"

	provider "github.com/valon-technologies/gestalt-providers/cache/valkey"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func main() {
	if err := gestalt.ServeCacheProvider(context.Background(), provider.New()); err != nil {
		log.Fatal(err)
	}
}
