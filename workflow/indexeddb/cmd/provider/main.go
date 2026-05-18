package main

import (
	"context"
	"log"

	provider "github.com/valon-technologies/gestalt-providers/workflow/indexeddb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func main() {
	if err := gestalt.ServeWorkflowProvider(context.Background(), provider.New()); err != nil {
		log.Fatal(err)
	}
}
