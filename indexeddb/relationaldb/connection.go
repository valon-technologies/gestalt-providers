package relationaldb

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"strings"
	"time"
)

const (
	defaultConnMaxLifetime = 30 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
	defaultPingTimeout     = 5 * time.Second
	defaultRetryAttempts   = 2
	defaultRetryBackoff    = 200 * time.Millisecond
	maxRetryBackoff        = 2 * time.Second
)

type connectionOptions struct {
	MaxOpenConns    *int
	MaxIdleConns    *int
	ConnMaxLifetime *time.Duration
	ConnMaxIdleTime *time.Duration
	PingTimeout     *time.Duration
	RetryAttempts   *int
	RetryBackoff    *time.Duration
}

func (o connectionOptions) apply(db *sql.DB) {
	if o.MaxOpenConns != nil {
		db.SetMaxOpenConns(*o.MaxOpenConns)
	}
	if o.MaxIdleConns != nil {
		db.SetMaxIdleConns(*o.MaxIdleConns)
	}
	db.SetConnMaxLifetime(o.connMaxLifetime())
	db.SetConnMaxIdleTime(o.connMaxIdleTime())
}

func openConfiguredDB(driver, connStr string, options connectionOptions) (*sql.DB, error) {
	db, err := sql.Open(driver, connStr)
	if err != nil {
		return nil, err
	}
	options.apply(db)
	return db, nil
}

func (o connectionOptions) connMaxLifetime() time.Duration {
	if o.ConnMaxLifetime != nil {
		return *o.ConnMaxLifetime
	}
	return defaultConnMaxLifetime
}

func (o connectionOptions) connMaxIdleTime() time.Duration {
	if o.ConnMaxIdleTime != nil {
		return *o.ConnMaxIdleTime
	}
	return defaultConnMaxIdleTime
}

func (o connectionOptions) pingTimeout() time.Duration {
	if o.PingTimeout != nil {
		return *o.PingTimeout
	}
	return defaultPingTimeout
}

func (o connectionOptions) retryAttempts() int {
	if o.RetryAttempts != nil {
		return *o.RetryAttempts
	}
	return defaultRetryAttempts
}

func (o connectionOptions) retryBackoff() time.Duration {
	if o.RetryBackoff != nil {
		return *o.RetryBackoff
	}
	return defaultRetryBackoff
}

func pingDatabase(ctx context.Context, db *sql.DB, options connectionOptions) error {
	return withRetry(ctx, options, func(attemptCtx context.Context) error {
		if options.pingTimeout() <= 0 {
			return db.PingContext(attemptCtx)
		}
		pingCtx, cancel := context.WithTimeout(attemptCtx, options.pingTimeout())
		defer cancel()
		return db.PingContext(pingCtx)
	})
}

func execWithRetry(ctx context.Context, db *sql.DB, options connectionOptions, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := withRetry(ctx, options, func(attemptCtx context.Context) error {
		var err error
		result, err = db.ExecContext(attemptCtx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func queryWithRetry(ctx context.Context, db *sql.DB, options connectionOptions, query string, args ...any) (*sql.Rows, error) {
	var rows *sql.Rows
	err := withRetry(ctx, options, func(attemptCtx context.Context) error {
		var err error
		rows, err = db.QueryContext(attemptCtx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func queryRowScanWithRetry(ctx context.Context, db *sql.DB, options connectionOptions, query string, args []any, scanDest ...any) error {
	return withRetry(ctx, options, func(attemptCtx context.Context) error {
		return db.QueryRowContext(attemptCtx, query, args...).Scan(scanDest...)
	})
}

func withRetry(ctx context.Context, options connectionOptions, fn func(context.Context) error) error {
	attempts := options.retryAttempts()
	backoff := options.retryBackoff()

	for attempt := 0; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if !isRetryableConnectionError(err) || attempt >= attempts {
			return err
		}
		if err := sleepContext(ctx, retryDelay(backoff, attempt)); err != nil {
			return err
		}
	}
}

func retryDelay(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	delay := base
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= maxRetryBackoff {
			return maxRetryBackoff
		}
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isRetryableConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		type temporary interface {
			Temporary() bool
		}
		if temp, ok := netErr.(temporary); ok && temp.Temporary() {
			return true
		}
	}

	message := strings.ToLower(err.Error())
	for _, needle := range []string{
		"driver: bad connection",
		"unexpected eof",
		"broken pipe",
		"connection reset",
		"connection refused",
		"connection aborted",
		"i/o timeout",
		"handshake failed",
		"dial tcp",
	} {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}
