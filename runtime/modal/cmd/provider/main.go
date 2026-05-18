package main

import (
	"context"
	"log"

	provider "github.com/valon-technologies/gestalt-providers/runtime/modal"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func main() {
	if err := gestalt.ServePluginRuntimeProvider(context.Background(), provider.New()); err != nil {
		log.Fatal(err)
	}
}
