package relationaldb

import (
	"context"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type Factory interface {
	Open(ctx context.Context, name string, opts OpenOptions) (Database, error)
	OpenCurrent(ctx context.Context, name string) (Database, error)
	DeleteDatabase(ctx context.Context, name string, opts ...DeleteOptions) error
	Databases(ctx context.Context) ([]DatabaseInfo, error)
	Cmp(a, b any) (int, error)
	Close() error
}

type Database interface {
	Name() string
	Version() uint64
	ObjectStoreNames(ctx context.Context) ([]string, error)
	Transaction(ctx context.Context, stores []string, mode gestalt.TransactionMode, opts gestalt.TransactionOptions) (gestalt.IndexedDBTransaction, error)
	Close() error
}

type OpenOptions struct {
	Version         *uint64
	Upgrade         func(context.Context, UpgradeContext) error
	OnVersionChange func(context.Context, VersionChangeInfo) error
	OnBlocked       func(context.Context, BlockedInfo) (BlockedAction, error)
}

type DeleteOptions struct {
	OnBlocked func(context.Context, BlockedInfo) (BlockedAction, error)
}

type UpgradeContext interface {
	OldVersion() uint64
	NewVersion() uint64
	Database() Database
	ObjectStoreNames(ctx context.Context) ([]string, error)
	ObjectStore(name string) (UpgradeObjectStore, error)
	CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreOptions) (UpgradeObjectStore, error)
	DeleteObjectStore(ctx context.Context, name string) error
}

type UpgradeObjectStore interface {
	Name() string
	Schema() gestalt.ObjectStoreOptions
	CreateIndex(ctx context.Context, name string, keyPath []string, params IndexParameters) error
	DeleteIndex(ctx context.Context, name string) error
}

type IndexParameters struct {
	Unique     bool
	MultiEntry bool
}

type DatabaseInfo struct {
	Name    string
	Version uint64
}

type VersionChangeInfo struct {
	Name       string
	OldVersion uint64
	NewVersion *uint64
	Reason     VersionChangeReason
}

type BlockedInfo struct {
	Name               string
	OldVersion         uint64
	NewVersion         *uint64
	Reason             VersionChangeReason
	OpenConnections    int
	ActiveTransactions int
}

type VersionChangeReason string

const (
	VersionChangeUpgrade VersionChangeReason = "upgrade"
	VersionChangeDelete  VersionChangeReason = "delete"
)

type BlockedAction int

const (
	BlockedFail BlockedAction = iota
	BlockedWait
)

type Options struct {
	Connection ConnectionOptions
	SQL        SQLOptions
}

type SQLOptions struct {
	Schema      string
	TablePrefix string
}

type ConnectionOptions = connectionOptions

type DriverConfig struct {
	Driver     string
	ConnString string
	Dialect    Dialect
	Bind       BindStyle
}

type Dialect string

const (
	DialectSQLite    Dialect = "sqlite"
	DialectPostgres  Dialect = "postgres"
	DialectMySQL     Dialect = "mysql"
	DialectSQLServer Dialect = "sqlserver"
)

type BindStyle int

const (
	BindQuestion BindStyle = iota
	BindDollar
	BindAtP
)

func ParseDSN(dsn string) DriverConfig {
	driver, connStr, style, d := parseDSN(dsn)
	return DriverConfig{
		Driver:     driver,
		ConnString: connStr,
		Dialect:    exportedDialect(d),
		Bind:       exportedBindStyle(style),
	}
}

func (o Options) storeOptions() storeOptions {
	return storeOptions{
		TablePrefix: strings.TrimSpace(o.SQL.TablePrefix),
		Schema:      strings.TrimSpace(o.SQL.Schema),
		Connection:  connectionOptions(o.Connection),
	}
}

func (c DriverConfig) internal() (string, string, bindStyle, dialect) {
	driver := c.Driver
	connStr := c.ConnString
	style := internalBindStyle(c.Bind)
	d := internalDialect(c.Dialect)
	if driver == "" {
		driver = defaultDriverForDialect(d)
	}
	return driver, connStr, style, d
}

func defaultDriverForDialect(d dialect) string {
	switch d {
	case dialectPostgres:
		return "pgx"
	case dialectMySQL:
		return "mysql"
	case dialectSQLServer:
		return "sqlserver"
	default:
		return "sqlite"
	}
}

func exportedDialect(d dialect) Dialect {
	switch d {
	case dialectPostgres:
		return DialectPostgres
	case dialectMySQL:
		return DialectMySQL
	case dialectSQLServer:
		return DialectSQLServer
	default:
		return DialectSQLite
	}
}

func internalDialect(d Dialect) dialect {
	switch d {
	case DialectPostgres:
		return dialectPostgres
	case DialectMySQL:
		return dialectMySQL
	case DialectSQLServer:
		return dialectSQLServer
	default:
		return dialectSQLite
	}
}

func exportedBindStyle(style bindStyle) BindStyle {
	switch style {
	case bindDollar:
		return BindDollar
	case bindAtP:
		return BindAtP
	default:
		return BindQuestion
	}
}

func internalBindStyle(style BindStyle) bindStyle {
	switch style {
	case BindDollar:
		return bindDollar
	case BindAtP:
		return bindAtP
	default:
		return bindQuestion
	}
}
