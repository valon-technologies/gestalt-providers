package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/valon-technologies/gestalt-providers/secrets/relationaldb/internal/secretstore"
)

const timeout = 30 * time.Second

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	flags := flag.NewFlagSet("gestalt-secrets", flag.ContinueOnError)
	schema := flags.String("schema", os.Getenv("GESTALT_SECRETS_SCHEMA"), "deployment schema (defaults to GESTALT_SECRETS_SCHEMA)")
	kmsKey := flags.String("kms-key", os.Getenv("GESTALT_SECRETS_KMS_KEY"), "Cloud KMS CryptoKey resource name (defaults to GESTALT_SECRETS_KMS_KEY)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	commandArgs := flags.Args()
	if len(commandArgs) == 0 {
		return fmt.Errorf("usage: gestalt-secrets [flags] init|list|put NAME|delete NAME")
	}

	var value []byte
	if commandArgs[0] == "put" {
		if len(commandArgs) != 2 {
			return fmt.Errorf("usage: gestalt-secrets [flags] put NAME")
		}
		var err error
		value, err = io.ReadAll(io.LimitReader(stdin, secretstore.MaxPlaintextBytes+1))
		if err != nil {
			return fmt.Errorf("read secret value: %w", err)
		}
	}

	openCtx, cancelOpen := context.WithTimeout(context.Background(), timeout)
	store, err := secretstore.Open(openCtx, secretstore.Config{DSN: os.Getenv("GESTALT_SECRETS_DSN"), Schema: *schema, KMSKey: *kmsKey})
	cancelOpen()
	if err != nil {
		return err
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch commandArgs[0] {
	case "init":
		if len(commandArgs) != 1 {
			return fmt.Errorf("usage: gestalt-secrets [flags] init")
		}
		return store.Initialize(ctx)
	case "list":
		if len(commandArgs) != 1 {
			return fmt.Errorf("usage: gestalt-secrets [flags] list")
		}
		names, err := store.List(ctx)
		if err != nil {
			return err
		}
		for _, name := range names {
			fmt.Fprintln(stdout, name)
		}
		return nil
	case "put":
		return store.Put(ctx, commandArgs[1], value)
	case "delete":
		if len(commandArgs) != 2 {
			return fmt.Errorf("usage: gestalt-secrets [flags] delete NAME")
		}
		return store.Delete(ctx, commandArgs[1])
	default:
		return fmt.Errorf("unknown command %q", commandArgs[0])
	}
}
