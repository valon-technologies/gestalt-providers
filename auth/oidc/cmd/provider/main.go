package main

import (
	"context"
	"log"

	provider "github.com/valon-technologies/gestalt-providers/auth/oidc"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func main() {
	if err := gestalt.ServeAuthenticationProvider(context.Background(), provider.New()); err != nil {
		log.Fatal(err)
	}
}
