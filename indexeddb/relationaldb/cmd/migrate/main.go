package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
)

func main() {
	var schema string
	var tablePrefix string
	flag.StringVar(&schema, "schema", "", "database schema containing the provider tables")
	flag.StringVar(&tablePrefix, "table-prefix", "", "prefix for the provider tables")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: migrate [--schema name] [--table-prefix prefix]")
		os.Exit(2)
	}

	dsn := strings.TrimSpace(os.Getenv("RELATIONALDB_DSN"))
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "RELATIONALDB_DSN is required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := relationaldb.Migrate(ctx, dsn, relationaldb.Options{SQL: relationaldb.SQLOptions{
		Schema:      schema,
		TablePrefix: tablePrefix,
	}}); err != nil {
		fmt.Fprintf(os.Stderr, "migrate relationaldb: %v\n", err)
		os.Exit(1)
	}
}
