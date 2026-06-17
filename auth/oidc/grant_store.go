package oidc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

const (
	grantStoreName          = "authentication_grants"
	tokenHashStoreName      = "authentication_token_hashes"
	grantIndexBySubject     = "by_subject"
	tokenIndexByGrantID     = "by_grant_id"
	grantCategorySession  = "session"
	grantCategoryAPIToken = "api_token"
)

const defaultOAuthClientID = "gestaltd"

type grantStore struct {
	db     indexeddb.Database
	grants indexeddb.ObjectStore
	tokens indexeddb.ObjectStore
	now    func() time.Time
}

type issuedGrant struct {
	grantID     string
	accessToken string
	expiresIn   int64
}

func openGrantStore(ctx context.Context, db indexeddb.Database, now func() time.Time) (*grantStore, error) {
	if db == nil {
		return nil, fmt.Errorf("indexeddb database is required")
	}
	if err := ensureGrantStores(ctx, db); err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	return &grantStore{
		db:     db,
		grants: db.ObjectStore(grantStoreName),
		tokens: db.ObjectStore(tokenHashStoreName),
		now:    now,
	}, nil
}

func ensureGrantStores(ctx context.Context, db indexeddb.Database) error {
	if _, err := db.CreateObjectStore(ctx, grantStoreName, grantStoreSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create %s store: %w", grantStoreName, err)
	}
	if _, err := db.CreateObjectStore(ctx, tokenHashStoreName, tokenHashStoreSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create %s store: %w", tokenHashStoreName, err)
	}
	return nil
}

func grantStoreSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: grantIndexBySubject, KeyPath: []string{"subject"}},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "subject", Type: gestalt.TypeString, NotNull: true},
			{Name: "scope", Type: gestalt.TypeString},
			{Name: "client_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "created_at", Type: gestalt.TypeTime, NotNull: true},
			{Name: "expires_at", Type: gestalt.TypeTime, NotNull: true},
			{Name: "revoked", Type: gestalt.TypeBool, NotNull: true},
			{Name: "category", Type: gestalt.TypeString, NotNull: true},
		},
	}
}

func tokenHashStoreSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: tokenIndexByGrantID, KeyPath: []string{"grant_id"}},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "grant_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "subject", Type: gestalt.TypeString, NotNull: true},
			{Name: "scope", Type: gestalt.TypeString},
			{Name: "client_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "expires_at", Type: gestalt.TypeTime, NotNull: true},
		},
	}
}

func (s *grantStore) currentTime() time.Time {
	return s.now().UTC()
}

func (s *grantStore) issue(ctx context.Context, subject, scope, clientID, category string, ttl time.Duration) (*issuedGrant, error) {
	if strings.TrimSpace(clientID) == "" {
		clientID = defaultOAuthClientID
	}
	if category == "" {
		category = grantCategorySession
	}
	now := s.currentTime()
	if ttl <= 0 {
		ttl = defaultSessionTTL
	}
	expiresAt := now.Add(ttl)
	grantID := "grant-" + uuid.NewString()
	accessToken := generateOpaqueToken()
	tokenHash := hashToken(accessToken)

	tx, err := s.db.Transaction(
		ctx,
		[]string{grantStoreName, tokenHashStoreName},
		indexeddb.TransactionReadwrite,
		indexeddb.TransactionOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: begin grant transaction: %w", err)
	}
	grantStore := tx.ObjectStore(grantStoreName)
	tokenStore := tx.ObjectStore(tokenHashStoreName)
	if err := grantStore.Add(ctx, gestalt.Record{
		"id":         grantID,
		"subject":    subject,
		"scope":      scope,
		"client_id":  clientID,
		"created_at": now,
		"expires_at": expiresAt,
		"revoked":    false,
		"category":   category,
	}); err != nil {
		_ = tx.Abort(ctx)
		return nil, fmt.Errorf("oidc auth: persist grant: %w", err)
	}
	if err := tokenStore.Add(ctx, gestalt.Record{
		"id":         tokenHash,
		"grant_id":   grantID,
		"subject":    subject,
		"scope":      scope,
		"client_id":  clientID,
		"expires_at": expiresAt,
	}); err != nil {
		_ = tx.Abort(ctx)
		return nil, fmt.Errorf("oidc auth: persist token hash: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("oidc auth: commit grant transaction: %w", err)
	}
	return &issuedGrant{
		grantID:     grantID,
		accessToken: accessToken,
		expiresIn:   int64(ttl.Seconds()),
	}, nil
}

func (s *grantStore) introspect(ctx context.Context, token string) gestalt.IntrospectResponse {
	record, err := s.tokens.Get(ctx, hashToken(token))
	if err != nil {
		return gestalt.IntrospectResponse{Active: false}
	}
	grantID := recordString(record, "grant_id")
	grantRecord, err := s.grants.Get(ctx, grantID)
	if err != nil || recordBool(grantRecord, "revoked") || !recordTime(grantRecord, "expires_at").After(s.currentTime()) {
		_ = s.tokens.Delete(ctx, hashToken(token))
		return gestalt.IntrospectResponse{Active: false}
	}
	if !recordTime(record, "expires_at").After(s.currentTime()) {
		_ = s.tokens.Delete(ctx, hashToken(token))
		return gestalt.IntrospectResponse{Active: false}
	}
	return gestalt.IntrospectResponse{
		Active:   true,
		Subject:  recordString(record, "subject"),
		Scope:    recordString(record, "scope"),
		ClientID: recordString(record, "client_id"),
	}
}

func (s *grantStore) listGrantIDs(ctx context.Context, subject string) []string {
	records, err := s.grants.Index(grantIndexBySubject).GetAll(ctx, subject)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil
	}
	if err != nil {
		return nil
	}
	now := s.currentTime()
	ids := make([]string, 0, len(records))
	for _, record := range records {
		if recordString(record, "category") != grantCategoryAPIToken {
			continue
		}
		if recordBool(record, "revoked") || !recordTime(record, "expires_at").After(now) {
			continue
		}
		if id := recordString(record, "id"); id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (s *grantStore) getGrant(ctx context.Context, grantID, subject string) (*gestalt.GetGrantResponse, error) {
	record, err := s.grants.Get(ctx, grantID)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return nil, grantNotFound(grantID)
		}
		return nil, fmt.Errorf("oidc auth: get grant %q: %w", grantID, err)
	}
	if recordString(record, "subject") != subject || recordBool(record, "revoked") {
		return nil, grantNotFound(grantID)
	}
	if recordString(record, "category") != grantCategoryAPIToken {
		return nil, grantNotFound(grantID)
	}
	if !recordTime(record, "expires_at").After(s.currentTime()) {
		return nil, grantNotFound(grantID)
	}
	return grantResponseFromRecord(record), nil
}

func (s *grantStore) revokeGrant(ctx context.Context, grantID, subject string) error {
	record, err := s.grants.Get(ctx, grantID)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return grantNotFound(grantID)
		}
		return fmt.Errorf("oidc auth: get grant %q: %w", grantID, err)
	}
	if recordString(record, "subject") != subject {
		return grantNotFound(grantID)
	}
	if recordString(record, "category") != grantCategoryAPIToken {
		return grantNotFound(grantID)
	}
	record["revoked"] = true
	if err := s.grants.Put(ctx, record); err != nil {
		return fmt.Errorf("oidc auth: revoke grant %q: %w", grantID, err)
	}
	tokenRecords, err := s.tokens.Index(tokenIndexByGrantID).GetAll(ctx, grantID)
	if err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return fmt.Errorf("oidc auth: list token hashes for grant %q: %w", grantID, err)
	}
	for _, tokenRecord := range tokenRecords {
		if id := recordString(tokenRecord, "id"); id != "" {
			_ = s.tokens.Delete(ctx, id)
		}
	}
	return nil
}

func grantNotFound(grantID string) error {
	return gestalt.NotFound(fmt.Sprintf("grant %q not found", grantID))
}

func grantResponseFromRecord(record gestalt.Record) *gestalt.GetGrantResponse {
	resp := &gestalt.GetGrantResponse{
		CreatedAt: recordTime(record, "created_at").Unix(),
		ExpiresAt: recordTime(record, "expires_at").Unix(),
	}
	if scope := strings.TrimSpace(recordString(record, "scope")); scope != "" {
		for _, part := range strings.Fields(scope) {
			resp.Scopes = append(resp.Scopes, gestalt.GrantScope{Scope: part})
		}
	}
	return resp
}

func generateOpaqueToken() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "tok-" + uuid.NewString()
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func subjectForVerifiedEmail(email string) string {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return ""
	}
	return "user:" + email
}

func recordString(record gestalt.Record, key string) string {
	value, _ := record[key].(string)
	return value
}

func recordBool(record gestalt.Record, key string) bool {
	value, _ := record[key].(bool)
	return value
}

func recordTime(record gestalt.Record, key string) time.Time {
	value, ok := record[key]
	if !ok || value == nil {
		return time.Time{}
	}
	switch raw := value.(type) {
	case time.Time:
		return raw.UTC()
	case *time.Time:
		if raw == nil {
			return time.Time{}
		}
		return raw.UTC()
	default:
		return time.Time{}
	}
}
