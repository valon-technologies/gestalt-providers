package main

import (
	"context"
	"log"

	provider "github.com/valon-technologies/gestalt-providers/secrets/onepassword"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func main() {
	if err := gestalt.ServeSecretsProvider(context.Background(), provider.New()); err != nil {
		log.Fatal(err)
	}
}
