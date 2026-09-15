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
	var backfillPrimaryKeys bool
	flag.StringVar(&schema, "schema", "", "database schema containing the provider tables")
	flag.StringVar(&tablePrefix, "table-prefix", "", "prefix for the provider tables")
	flag.BoolVar(&backfillPrimaryKeys, "backfill-primary-keys", false, "fill legacy ordered primary keys after provisioning; safe to resume")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: migrate [--schema name] [--table-prefix prefix] [--backfill-primary-keys]")
		os.Exit(2)
	}

	dsn := strings.TrimSpace(os.Getenv("RELATIONALDB_DSN"))
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "RELATIONALDB_DSN is required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if backfillPrimaryKeys {
		updated, err := relationaldb.BackfillPrimaryKeyOrder(ctx, dsn, relationaldb.Options{SQL: relationaldb.SQLOptions{Schema: schema, TablePrefix: tablePrefix}})
		if err != nil {
			fmt.Fprintf(os.Stderr, "backfill primary keys after %d updates: %v\n", updated, err)
			os.Exit(1)
		}
		fmt.Printf("backfilled %d primary keys\n", updated)
		return
	}
	if err := relationaldb.Migrate(ctx, dsn, relationaldb.Options{SQL: relationaldb.SQLOptions{
		Schema:      schema,
		TablePrefix: tablePrefix,
	}}); err != nil {
		fmt.Fprintf(os.Stderr, "migrate relationaldb: %v\n", err)
		os.Exit(1)
	}
}
