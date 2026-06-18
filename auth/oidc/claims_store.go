package oidc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

const subjectClaimsStoreName = "authentication_subject_claims"

type subjectClaimsRecord struct {
	Subject     string
	Email       string
	Name        string
	Issuer      string
	UpstreamSub string
	UpdatedAt   time.Time
}

type claimsStore struct {
	claims indexeddb.ObjectStore
	now    func() time.Time
}

func openClaimsStore(ctx context.Context, db indexeddb.Database, now func() time.Time) (*claimsStore, error) {
	if db == nil {
		return nil, fmt.Errorf("indexeddb database is required")
	}
	if err := ensureClaimsStore(ctx, db); err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	return &claimsStore{
		claims: db.ObjectStore(subjectClaimsStoreName),
		now:    now,
	}, nil
}

func ensureClaimsStore(ctx context.Context, db indexeddb.Database) error {
	if _, err := db.CreateObjectStore(ctx, subjectClaimsStoreName, subjectClaimsStoreSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create %s store: %w", subjectClaimsStoreName, err)
	}
	return nil
}

func subjectClaimsStoreSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "subject", Type: gestalt.TypeString, NotNull: true},
			{Name: "email", Type: gestalt.TypeString, NotNull: true},
			{Name: "name", Type: gestalt.TypeString},
			{Name: "issuer", Type: gestalt.TypeString},
			{Name: "upstream_sub", Type: gestalt.TypeString},
			{Name: "updated_at", Type: gestalt.TypeTime, NotNull: true},
		},
	}
}

func (s *claimsStore) upsert(ctx context.Context, record subjectClaimsRecord) error {
	subject := strings.TrimSpace(record.Subject)
	if subject == "" {
		return fmt.Errorf("oidc auth: claims subject is required")
	}
	email := strings.TrimSpace(record.Email)
	if email == "" {
		return fmt.Errorf("oidc auth: claims email is required")
	}

	existingName := ""
	if existing, err := s.get(ctx, subject); err == nil && existing != nil {
		existingName = strings.TrimSpace(existing.Name)
	} else if err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return err
	}

	name := strings.TrimSpace(record.Name)
	if name == "" {
		name = existingName
	}

	updatedAt := s.now().UTC()
	if !record.UpdatedAt.IsZero() {
		updatedAt = record.UpdatedAt.UTC()
	}
	rec := indexeddb.Record{
		"id":           subject,
		"subject":      subject,
		"email":        email,
		"name":         name,
		"issuer":       strings.TrimSpace(record.Issuer),
		"upstream_sub": strings.TrimSpace(record.UpstreamSub),
		"updated_at":   updatedAt,
	}
	if err := s.claims.Put(ctx, rec); err != nil {
		return fmt.Errorf("oidc auth: persist subject claims: %w", err)
	}
	return nil
}

func (s *claimsStore) get(ctx context.Context, subject string) (*subjectClaimsRecord, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return nil, gestalt.ErrNotFound
	}
	rec, err := s.claims.Get(ctx, subject)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return nil, gestalt.ErrNotFound
		}
		return nil, fmt.Errorf("oidc auth: get subject claims: %w", err)
	}
	return recordToSubjectClaims(rec), nil
}

func recordToSubjectClaims(rec indexeddb.Record) *subjectClaimsRecord {
	updatedAt, _ := rec["updated_at"].(time.Time)
	return &subjectClaimsRecord{
		Subject:     claimsString(rec, "subject"),
		Email:       claimsString(rec, "email"),
		Name:        claimsString(rec, "name"),
		Issuer:      claimsString(rec, "issuer"),
		UpstreamSub: claimsString(rec, "upstream_sub"),
		UpdatedAt:   updatedAt,
	}
}

func claimsString(rec indexeddb.Record, key string) string {
	value, ok := rec[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}
