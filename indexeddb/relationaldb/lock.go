package relationaldb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const lockTableName = "_gestalt_locks"

func (s *Store) locksTable() string {
	return qualifyTableName(s.schemaName, s.tablePrefix+lockTableName)
}

func lockTableSQL(d dialect, table string) string {
	defs := []string{
		quoteIdent(d, "lock_key") + " " + sqlType(d, 0, true) + " NOT NULL PRIMARY KEY",
		quoteIdent(d, "holder") + " " + sqlType(d, 0, false) + " NOT NULL",
		quoteIdent(d, "expires_at") + " " + sqlType(d, 1, false) + " NOT NULL",
		quoteIdent(d, "fencing_token") + " " + sqlType(d, 1, false) + " NOT NULL",
	}
	if d == dialectSQLServer {
		return fmt.Sprintf("IF OBJECT_ID(N'%s', N'U') IS NULL CREATE TABLE %s (%s)",
			table, quoteTableName(d, table), strings.Join(defs, ", "))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteTableName(d, table), strings.Join(defs, ", "))
}

// acquireLock atomically claims a keyed lease, reclaiming it if the current
// lease has expired. Exactly one caller wins even when several observe the same
// expired lease, because the conditional upsert serializes on the row.
func (s *Store) acquireLock(ctx context.Context, key, holder string, ttl time.Duration) (gestalt.IndexedDBLockLease, error) {
	if s.dialect == dialectSQLServer {
		return gestalt.IndexedDBLockLease{}, gestalt.Unimplemented("relationaldb: advisory locks are not supported on the sqlserver dialect")
	}

	nowMs := time.Now().UnixMilli()
	newExpiresMs := nowMs + ttl.Milliseconds()

	var lease gestalt.IndexedDBLockLease
	err := s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		txCtx = contextWithTx(txCtx, tx, nil)
		if err := s.execAcquireLock(txCtx, key, holder, newExpiresMs, nowMs); err != nil {
			return err
		}
		var (
			curHolder    string
			curExpiresMs int64
			curFencing   int64
		)
		err := s.scanOne(txCtx,
			"SELECT "+quoteIdent(s.dialect, "holder")+", "+quoteIdent(s.dialect, "expires_at")+", "+quoteIdent(s.dialect, "fencing_token")+
				" FROM "+quoteTableName(s.dialect, s.locksTable())+
				" WHERE "+quoteIdent(s.dialect, "lock_key")+" = ?",
			[]any{key}, &curHolder, &curExpiresMs, &curFencing,
		)
		if err != nil {
			return err
		}
		lease = gestalt.IndexedDBLockLease{
			Acquired:     curHolder == holder,
			Holder:       curHolder,
			ExpiresAt:    time.UnixMilli(curExpiresMs).UTC(),
			FencingToken: curFencing,
		}
		return nil
	})
	if err != nil {
		return gestalt.IndexedDBLockLease{}, err
	}
	return lease, nil
}

func (s *Store) execAcquireLock(ctx context.Context, key, holder string, newExpiresMs, nowMs int64) error {
	table := quoteTableName(s.dialect, s.locksTable())
	lockKey := quoteIdent(s.dialect, "lock_key")
	holderCol := quoteIdent(s.dialect, "holder")
	expiresCol := quoteIdent(s.dialect, "expires_at")
	fencingCol := quoteIdent(s.dialect, "fencing_token")

	if s.dialect == dialectMySQL {
		// Update only when the existing lease has expired or is held by the same
		// holder (a renew). The IF conditions all read the pre-update expires_at
		// because expires_at is assigned last.
		cond := expiresCol + " < ? OR " + holderCol + " = VALUES(" + holderCol + ")"
		stmt := "INSERT INTO " + table + " (" + lockKey + ", " + holderCol + ", " + expiresCol + ", " + fencingCol + ") VALUES (?, ?, ?, 1) " +
			"ON DUPLICATE KEY UPDATE " +
			fencingCol + " = IF(" + cond + ", " + fencingCol + " + 1, " + fencingCol + "), " +
			holderCol + " = IF(" + cond + ", VALUES(" + holderCol + "), " + holderCol + "), " +
			expiresCol + " = IF(" + cond + ", VALUES(" + expiresCol + "), " + expiresCol + ")"
		_, err := s.exec(ctx, stmt, key, holder, newExpiresMs, nowMs, nowMs, nowMs)
		return err
	}

	// Postgres and SQLite: conditional upsert. The DO UPDATE fires only when the
	// existing lease has expired or is held by the same holder (a renew);
	// concurrent callers serialize on the row, so a live lease held by another
	// holder is never stolen.
	stmt := "INSERT INTO " + table + " (" + lockKey + ", " + holderCol + ", " + expiresCol + ", " + fencingCol + ") VALUES (?, ?, ?, 1) " +
		"ON CONFLICT (" + lockKey + ") DO UPDATE SET " +
		holderCol + " = excluded." + holderCol + ", " +
		expiresCol + " = excluded." + expiresCol + ", " +
		fencingCol + " = " + fencingCol + " + 1 " +
		"WHERE " + expiresCol + " < ? OR " + holderCol + " = excluded." + holderCol
	_, err := s.exec(ctx, stmt, key, holder, newExpiresMs, nowMs)
	return err
}

// releaseLock drops the lease only if it is still held by holder.
func (s *Store) releaseLock(ctx context.Context, key, holder string) error {
	if s.dialect == dialectSQLServer {
		return gestalt.Unimplemented("relationaldb: advisory locks are not supported on the sqlserver dialect")
	}
	_, err := s.exec(ctx,
		"DELETE FROM "+quoteTableName(s.dialect, s.locksTable())+
			" WHERE "+quoteIdent(s.dialect, "lock_key")+" = ? AND "+quoteIdent(s.dialect, "holder")+" = ?",
		key, holder,
	)
	return err
}

// AcquireLock claims a keyed advisory lease, implementing gestalt.IndexedDBLockProvider.
func (p *Provider) AcquireLock(ctx context.Context, req gestalt.IndexedDBAcquireLockRequest) (gestalt.IndexedDBLockLease, error) {
	if p.Store == nil {
		return gestalt.IndexedDBLockLease{}, fmt.Errorf("relationaldb: store is not configured")
	}
	return p.Store.acquireLock(ctx, req.Key, req.Holder, req.TTL)
}

// ReleaseLock releases a keyed advisory lease held by req.Holder.
func (p *Provider) ReleaseLock(ctx context.Context, req gestalt.IndexedDBReleaseLockRequest) error {
	if p.Store == nil {
		return fmt.Errorf("relationaldb: store is not configured")
	}
	return p.Store.releaseLock(ctx, req.Key, req.Holder)
}

var _ gestalt.IndexedDBLockProvider = (*Provider)(nil)
