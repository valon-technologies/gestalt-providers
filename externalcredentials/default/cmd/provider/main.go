package main

import (
	"context"
	"log"

	provider "github.com/valon-technologies/gestalt-providers/externalcredentials/default"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func main() {
	if err := gestalt.ServeExternalCredentialProvider(context.Background(), provider.New()); err != nil {
		log.Fatal(err)
	}
}
