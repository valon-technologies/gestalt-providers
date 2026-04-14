package relationaldb

import (
	"database/sql"
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
		if err := ensureMySQLDatabase(connStr); err != nil {
			return err
		}
	case dialectPostgres:
		if err := ensurePostgresDatabase(connStr); err != nil {
			return err
		}
	case dialectSQLServer:
		if err := ensureSQLServerDatabase(connStr); err != nil {
			return err
		}
	}

	db, err := sql.Open(driver, connStr)
	if err != nil {
		return fmt.Errorf("relationaldb: open: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("relationaldb: ping: %w", err)
	}
	if err := ensureRelationalNamespace(db, d, options.Schema); err != nil {
		return err
	}
	return nil
}

func ensureMySQLDatabase(connStr string) error {
	cfg, err := mysqlcfg.ParseDSN(connStr)
	if err != nil {
		return fmt.Errorf("relationaldb: parse mysql dsn: %w", err)
	}
	if cfg.DBName == "" {
		return nil
	}

	target := cfg.DBName
	cfg.DBName = ""

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("relationaldb: open mysql admin connection: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("relationaldb: ping mysql admin connection: %w", err)
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + quoteIdent(dialectMySQL, target)); err != nil {
		return fmt.Errorf("relationaldb: create mysql database %q: %w", target, err)
	}
	return nil
}

func ensurePostgresDatabase(connStr string) error {
	target, adminConnStr, err := postgresAdminConnStr(connStr)
	if err != nil {
		return err
	}
	if target == "" || strings.EqualFold(target, "postgres") {
		return nil
	}

	db, err := sql.Open("pgx", adminConnStr)
	if err != nil {
		return fmt.Errorf("relationaldb: open postgres admin connection: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("relationaldb: ping postgres admin connection: %w", err)
	}

	var exists bool
	if err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", target).Scan(&exists); err != nil {
		return fmt.Errorf("relationaldb: check postgres database %q: %w", target, err)
	}
	if exists {
		return nil
	}
	if _, err := db.Exec("CREATE DATABASE " + quoteIdent(dialectPostgres, target)); err != nil {
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

func ensureSQLServerDatabase(connStr string) error {
	target, adminConnStr, err := sqlServerAdminConnStr(connStr)
	if err != nil {
		return err
	}
	if target == "" || strings.EqualFold(target, "master") {
		return nil
	}

	db, err := sql.Open("sqlserver", adminConnStr)
	if err != nil {
		return fmt.Errorf("relationaldb: open sqlserver admin connection: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("relationaldb: ping sqlserver admin connection: %w", err)
	}

	var exists int
	if err := db.QueryRow("SELECT COUNT(1) FROM sys.databases WHERE name = @p1", target).Scan(&exists); err != nil {
		return fmt.Errorf("relationaldb: check sqlserver database %q: %w", target, err)
	}
	if exists > 0 {
		return nil
	}
	if _, err := db.Exec("CREATE DATABASE " + quoteIdent(dialectSQLServer, target)); err != nil {
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

func ensureRelationalNamespace(db *sql.DB, d dialect, schema string) error {
	if schema == "" {
		return nil
	}

	var query string
	switch d {
	case dialectMySQL:
		query = "CREATE DATABASE IF NOT EXISTS " + quoteIdent(dialectMySQL, schema)
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

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("relationaldb: ensure namespace %q: %w", schema, err)
	}
	return nil
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
