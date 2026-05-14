package relationaldb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	mysqlcfg "github.com/go-sql-driver/mysql"
)

func ensureRelationalTargetExists(dsn string, options storeOptions) error {
	driver, connStr, _, d := parseDSN(dsn)
	switch d {
	case dialectSQLite:
		return nil
	case dialectMySQL:
		if err := ensureMySQLDatabase(connStr, options.Connection); err != nil {
			return err
		}
	case dialectPostgres:
		if err := ensurePostgresDatabase(connStr, options.Connection); err != nil {
			return err
		}
	case dialectSQLServer:
		if err := ensureSQLServerDatabase(connStr, options.Connection); err != nil {
			return err
		}
	}

	db, err := openConfiguredDB(driver, connStr, options.Connection)
	if err != nil {
		return fmt.Errorf("relationaldb: open: %w", err)
	}
	defer db.Close()

	if err := pingDatabase(context.Background(), db, options.Connection); err != nil {
		return fmt.Errorf("relationaldb: ping: %w", err)
	}
	if err := ensureRelationalNamespace(context.Background(), db, d, options.Schema, options.Connection); err != nil {
		return err
	}
	return nil
}

func ensureMySQLDatabase(connStr string, options connectionOptions) error {
	cfg, err := mysqlcfg.ParseDSN(connStr)
	if err != nil {
		return fmt.Errorf("relationaldb: parse mysql dsn: %w", err)
	}
	if cfg.DBName == "" {
		return nil
	}

	target := cfg.DBName
	cfg.DBName = ""

	db, err := openConfiguredDB("mysql", cfg.FormatDSN(), options)
	if err != nil {
		return fmt.Errorf("relationaldb: open mysql admin connection: %w", err)
	}
	defer db.Close()

	if err := pingDatabase(context.Background(), db, options); err != nil {
		return fmt.Errorf("relationaldb: ping mysql admin connection: %w", err)
	}
	return ensureMySQLSchemaExists(context.Background(), db, target, options)
}

func ensurePostgresDatabase(connStr string, options connectionOptions) error {
	target, adminConnStr, err := postgresAdminConnStr(connStr)
	if err != nil {
		return err
	}
	if target == "" || strings.EqualFold(target, "postgres") {
		return nil
	}

	db, err := openConfiguredDB("pgx", adminConnStr, options)
	if err != nil {
		return fmt.Errorf("relationaldb: open postgres admin connection: %w", err)
	}
	defer db.Close()

	if err := pingDatabase(context.Background(), db, options); err != nil {
		return fmt.Errorf("relationaldb: ping postgres admin connection: %w", err)
	}

	var exists bool
	if err := queryRowScanWithRetry(context.Background(), db, options, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", []any{target}, &exists); err != nil {
		return fmt.Errorf("relationaldb: check postgres database %q: %w", target, err)
	}
	if exists {
		return nil
	}
	if _, err := execWithRetry(context.Background(), db, options, "CREATE DATABASE "+quoteIdent(dialectPostgres, target)); err != nil {
		return fmt.Errorf("relationaldb: create postgres database %q: %w", target, err)
	}
	return nil
}

func postgresAdminConnStr(connStr string) (targetDB string, adminConnStr string, err error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", "", fmt.Errorf("relationaldb: parse postgres dsn: %w", err)
	}
	targetDB = strings.TrimPrefix(u.Path, "/")
	adminURL := *u
	adminURL.Path = "/postgres"
	return targetDB, adminURL.String(), nil
}

func ensureSQLServerDatabase(connStr string, options connectionOptions) error {
	target, adminConnStr, err := sqlServerAdminConnStr(connStr)
	if err != nil {
		return err
	}
	if target == "" || strings.EqualFold(target, "master") {
		return nil
	}

	db, err := openConfiguredDB("sqlserver", adminConnStr, options)
	if err != nil {
		return fmt.Errorf("relationaldb: open sqlserver admin connection: %w", err)
	}
	defer db.Close()

	if err := pingDatabase(context.Background(), db, options); err != nil {
		return fmt.Errorf("relationaldb: ping sqlserver admin connection: %w", err)
	}

	var exists int
	if err := queryRowScanWithRetry(context.Background(), db, options, "SELECT COUNT(1) FROM sys.databases WHERE name = @p1", []any{target}, &exists); err != nil {
		return fmt.Errorf("relationaldb: check sqlserver database %q: %w", target, err)
	}
	if exists > 0 {
		return nil
	}
	if _, err := execWithRetry(context.Background(), db, options, "CREATE DATABASE "+quoteIdent(dialectSQLServer, target)); err != nil {
		return fmt.Errorf("relationaldb: create sqlserver database %q: %w", target, err)
	}
	return nil
}

func sqlServerAdminConnStr(connStr string) (targetDB string, adminConnStr string, err error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", "", fmt.Errorf("relationaldb: parse sqlserver dsn: %w", err)
	}
	q := u.Query()
	targetDB = q.Get("database")
	q.Set("database", "master")
	u.RawQuery = q.Encode()
	return targetDB, u.String(), nil
}

func ensureRelationalNamespace(ctx context.Context, db *sql.DB, d dialect, schema string, options connectionOptions) error {
	if schema == "" {
		return nil
	}

	var query string
	switch d {
	case dialectMySQL:
		return ensureMySQLSchemaExists(ctx, db, schema, options)
	case dialectPostgres:
		query = "CREATE SCHEMA IF NOT EXISTS " + quoteIdent(dialectPostgres, schema)
	case dialectSQLServer:
		query = fmt.Sprintf(
			"IF SCHEMA_ID(N%s) IS NULL EXEC(N'CREATE SCHEMA %s')",
			sqlStringLiteral(schema),
			strings.ReplaceAll(quoteIdent(dialectSQLServer, schema), "'", "''"),
		)
	default:
		return nil
	}

	if _, err := execWithRetry(ctx, db, options, query); err != nil {
		return fmt.Errorf("relationaldb: ensure namespace %q: %w", schema, err)
	}
	return nil
}

func ensureMySQLSchemaExists(ctx context.Context, db *sql.DB, schema string, options connectionOptions) error {
	var exists bool
	if err := queryRowScanWithRetry(
		ctx,
		db,
		options,
		"SELECT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?)",
		[]any{schema},
		&exists,
	); err != nil {
		return fmt.Errorf("relationaldb: check mysql schema %q: %w", schema, err)
	}
	if exists {
		return nil
	}
	if _, err := execWithRetry(ctx, db, options, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(dialectMySQL, schema)); err != nil {
		if isVitessCreateDatabaseUnsupported(err) {
			return fmt.Errorf("relationaldb: mysql schema %q does not exist and CREATE DATABASE is not supported; create the database out of band before configuring relationaldb to use it (PlanetScale/Vitess does not allow CREATE DATABASE through normal SQL connections): %w", schema, err)
		}
		return fmt.Errorf("relationaldb: create mysql schema %q: %w", schema, err)
	}
	return nil
}

func isVitessCreateDatabaseUnsupported(err error) bool {
	if err == nil {
		return false
	}

	var myErr *mysqlcfg.MySQLError
	if errors.As(err, &myErr) {
		if strings.Contains(myErr.Message, "VT12001") && strings.Contains(myErr.Message, "failDBDDL") {
			return true
		}
	}

	message := err.Error()
	return strings.Contains(message, "VT12001") && strings.Contains(message, "failDBDDL")
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
