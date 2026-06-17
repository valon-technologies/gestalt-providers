package oidc

import (
	"context"
	"fmt"
	"sort"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

const legacyAPITokenStoreName = "api_tokens"

func loadLegacyAPITokenRows(ctx context.Context, db indexeddb.Database, limit int) ([]LegacyAPITokenRow, error) {
	if db == nil {
		return nil, fmt.Errorf("indexeddb database is required")
	}
	records, err := db.ObjectStore(legacyAPITokenStoreName).GetAll(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("read legacy %s object store: %w", legacyAPITokenStoreName, err)
	}
	rows := make([]LegacyAPITokenRow, 0, len(records))
	for _, record := range records {
		row, err := legacyRowFromRecord(record)
		if err != nil {
			return rows, err
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ID < rows[j].ID
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func legacyRowFromRecord(record gestalt.Record) (LegacyAPITokenRow, error) {
	id := strings.TrimSpace(recordString(record, "id"))
	if id == "" {
		return LegacyAPITokenRow{}, fmt.Errorf("legacy api token record missing id")
	}
	row := LegacyAPITokenRow{
		ID:                  id,
		OwnerKind:           recordString(record, "owner_kind"),
		OwnerID:             recordString(record, "owner_id"),
		CredentialSubjectID: recordString(record, "credential_subject_id"),
		HashedToken:         recordString(record, "hashed_token"),
		Scopes:              recordString(record, "scopes"),
		PermissionsJSON:     recordString(record, "permissions_json"),
		CreatedAt:           recordTime(record, "created_at"),
		UpdatedAt:           recordTime(record, "updated_at"),
	}
	if expiresAt := recordTime(record, "expires_at"); !expiresAt.IsZero() {
		exp := expiresAt.UTC()
		row.ExpiresAt = &exp
	}
	return row, nil
}
