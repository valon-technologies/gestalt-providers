package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	DefaultMaxOpenConns    = 25
	DefaultMaxIdleConns    = 5
	DefaultConnMaxLifetime = 5 * time.Minute
)

func Open(driverName, dsn string, d Dialect) (*Store, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", driverName, err)
	}
	return OpenDB(db, driverName, d)
}

func OpenDB(db *sql.DB, driverName string, d Dialect) (*Store, error) {
	db.SetMaxOpenConns(DefaultMaxOpenConns)
	db.SetMaxIdleConns(DefaultMaxIdleConns)
	db.SetConnMaxLifetime(DefaultConnMaxLifetime)

	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pinging %s: %w", driverName, err)
	}

	return New(db, d), nil
}

func OpenVersioned(driverName, dsn string, d Dialect, requestedVersion string, resolve func(context.Context, *sql.DB, string) (string, error)) (*Store, error) {
	s, err := Open(driverName, dsn, d)
	if err != nil {
		return nil, err
	}
	if _, err := resolve(context.Background(), s.DB, requestedVersion); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}
