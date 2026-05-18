package main

import (
	"context"
	"log"

	provider "github.com/valon-technologies/gestalt-providers/authorization/indexeddb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func main() {
	if err := gestalt.ServeAuthorizationProvider(context.Background(), provider.New()); err != nil {
		log.Fatal(err)
	}
}
