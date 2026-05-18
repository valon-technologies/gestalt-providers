package main

import (
	"context"
	"log"

	provider "github.com/valon-technologies/gestalt-providers/s3/s3"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func main() {
	if err := gestalt.ServeS3Provider(context.Background(), provider.New()); err != nil {
		log.Fatal(err)
	}
}
