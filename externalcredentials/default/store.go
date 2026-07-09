package externalcredentials

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

const (
	storeName              = "external_credentials"
	indexBySubject         = "by_subject"
	indexBySubjectAudience = "by_subject_audience"
	indexByKey             = "by_key"

	kindGrant      = "grant"
	kindClientInfo = "client_info"
	kindOpaque     = "opaque"
)

type store struct {
	client      indexeddb.Database
	credentials indexeddb.ObjectStore
	encryptor   *aesgcmEncryptor
}

func openStore(ctx context.Context, cfg config, client indexeddb.Database) (*store, error) {
	if client == nil {
		return nil, fmt.Errorf("indexeddb database is required")
	}

	encryptor, err := newEncryptorFromConfig(cfg.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("build encryptor: %w", err)
	}

	st := &store{
		client:      client,
		credentials: client.ObjectStore(storeName),
		encryptor:   encryptor,
	}
	return st, nil
}

func externalCredentialSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: indexBySubject, KeyPath: []string{"subject"}},
			{Name: indexBySubjectAudience, KeyPath: []string{"subject", "audience"}},
			{Name: indexByKey, KeyPath: []string{"subject", "audience", "qualifier"}, Unique: true},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "subject", Type: gestalt.TypeString, NotNull: true},
			{Name: "audience", Type: gestalt.TypeString, NotNull: true},
			{Name: "qualifier", Type: gestalt.TypeString},
			{Name: "kind", Type: gestalt.TypeString, NotNull: true},
			{Name: "access_token_encrypted", Type: gestalt.TypeString},
			{Name: "refresh_token_encrypted", Type: gestalt.TypeString},
			{Name: "scope", Type: gestalt.TypeString},
			{Name: "expires_at", Type: gestalt.TypeTime},
			{Name: "last_refreshed_at", Type: gestalt.TypeTime},
			{Name: "refresh_error_count", Type: gestalt.TypeInt},
			{Name: "client_id", Type: gestalt.TypeString},
			{Name: "client_secret_encrypted", Type: gestalt.TypeString},
			{Name: "client_secret_expires_at", Type: gestalt.TypeTime},
			{Name: "fields_encrypted", Type: gestalt.TypeString},
			{Name: "metadata_json", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "updated_at", Type: gestalt.TypeTime},
		},
	}
}

func (s *store) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *store) createCredential(ctx context.Context, credential *gestalt.ExternalCredential, now time.Time) (*gestalt.ExternalCredential, error) {
	normalized, err := normalizeCredential(credential)
	if err != nil {
		return nil, err
	}
	if normalized.GetId() == "" {
		normalized.ID = uuid.NewString()
	}

	record, err := s.credentialToRecord(normalized)
	if err != nil {
		return nil, err
	}
	record["created_at"] = now.UTC()
	record["updated_at"] = now.UTC()

	if err := s.credentials.Add(ctx, record); err != nil {
		if errors.Is(err, gestalt.ErrAlreadyExists) {
			return nil, gestalt.ErrAlreadyExists
		}
		return nil, fmt.Errorf("create external credential: %w", err)
	}
	return s.getCredential(ctx, normalized.GetSubject(), normalized.GetAudience(), normalized.GetQualifier())
}

func (s *store) upsertCredential(ctx context.Context, credential *gestalt.ExternalCredential, now time.Time) (*gestalt.ExternalCredential, error) {
	normalized, err := normalizeCredential(credential)
	if err != nil {
		return nil, err
	}

	record, err := s.credentialToRecord(normalized)
	if err != nil {
		return nil, err
	}
	record["updated_at"] = now.UTC()

	existing, err := s.credentialRecord(ctx, normalized.GetSubject(), normalized.GetAudience(), normalized.GetQualifier())
	switch {
	case err == nil:
		record["id"] = recordString(existing, "id")
		createdAt := recordTime(existing, "created_at")
		if createdAt.IsZero() {
			createdAt = now.UTC()
		}
		record["created_at"] = createdAt
		if err := s.credentials.Put(ctx, record); err != nil {
			return nil, fmt.Errorf("update external credential: %w", err)
		}
	case errors.Is(err, gestalt.ErrExternalCredentialNotFound):
		if normalized.GetId() == "" {
			normalized.ID = uuid.NewString()
			record["id"] = normalized.GetId()
		}
		record["created_at"] = now.UTC()
		if err := s.credentials.Add(ctx, record); err != nil {
			return nil, fmt.Errorf("create external credential: %w", err)
		}
	default:
		return nil, fmt.Errorf("check existing external credential: %w", err)
	}

	return s.getCredential(ctx, normalized.GetSubject(), normalized.GetAudience(), normalized.GetQualifier())
}

func (s *store) getCredential(ctx context.Context, subject, audience, qualifier string) (*gestalt.ExternalCredential, error) {
	record, err := s.credentialRecord(ctx, subject, audience, qualifier)
	if err != nil {
		return nil, err
	}
	return s.recordToCredential(record)
}

func (s *store) listCredentials(ctx context.Context, subject, audience string) ([]*gestalt.ExternalCredential, error) {
	var (
		records []gestalt.Record
		err     error
	)
	switch {
	case audience != "":
		records, err = s.listCredentialRecords(ctx, indexBySubjectAudience, subject, audience)
	default:
		records, err = s.listCredentialRecords(ctx, indexBySubject, subject)
	}
	if err != nil {
		return nil, err
	}

	credentials := make([]*gestalt.ExternalCredential, 0, len(records))
	for _, record := range records {
		credential, err := s.recordToCredential(record)
		if err != nil {
			return nil, err
		}
		credentials = append(credentials, credential)
	}
	return credentials, nil
}

func (s *store) listCredentialsForAudiences(ctx context.Context, audiences map[string]struct{}) ([]*gestalt.ExternalCredential, error) {
	if len(audiences) == 0 {
		return nil, nil
	}
	records, err := s.credentials.GetAll(ctx, nil)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	credentials := make([]*gestalt.ExternalCredential, 0, len(records))
	for _, record := range records {
		if _, ok := audiences[recordString(record, "audience")]; !ok {
			continue
		}
		credential, err := s.recordToCredential(record)
		if err != nil {
			return nil, err
		}
		credentials = append(credentials, credential)
	}
	return credentials, nil
}

func (s *store) deleteCredential(ctx context.Context, id string) error {
	if err := s.credentials.Delete(ctx, id); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return fmt.Errorf("delete external credential %q: %w", id, err)
	}
	return nil
}

func (s *store) credentialRecord(ctx context.Context, subject, audience, qualifier string) (gestalt.Record, error) {
	record, err := s.credentials.Index(indexByKey).Get(ctx, []any{subject, audience, qualifier})
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, gestalt.ErrExternalCredentialNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get external credential: %w", err)
	}
	return record, nil
}

func (s *store) listCredentialRecords(ctx context.Context, indexName string, keys ...any) ([]gestalt.Record, error) {
	var query any
	if len(keys) == 1 {
		query = keys[0]
	} else if len(keys) > 1 {
		query = keys
	}
	records, err := s.credentials.Index(indexName).GetAll(ctx, query)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (s *store) credentialToRecord(credential *gestalt.ExternalCredential) (gestalt.Record, error) {
	record := gestalt.Record{
		"id":            credential.GetId(),
		"subject":       credential.GetSubject(),
		"audience":      credential.GetAudience(),
		"qualifier":     credential.GetQualifier(),
		"metadata_json": credential.GetMetadataJson(),
	}
	switch {
	case credential.Grant != nil:
		accessEnc, refreshEnc, err := s.encryptor.EncryptTokenPair(credential.Grant.GetAccessToken(), credential.Grant.GetRefreshToken())
		if err != nil {
			return nil, fmt.Errorf("encrypt grant tokens: %w", err)
		}
		record["kind"] = kindGrant
		record["access_token_encrypted"] = accessEnc
		record["refresh_token_encrypted"] = refreshEnc
		record["scope"] = credential.Grant.GetScope()
		record["expires_at"] = utcTimePtr(credential.Grant.GetExpiresAt())
		record["last_refreshed_at"] = utcTimePtr(credential.Grant.GetLastRefreshedAt())
		record["refresh_error_count"] = credential.Grant.GetRefreshErrorCount()
	case credential.Client != nil:
		secretEnc, err := s.encryptor.Encrypt(credential.Client.GetClientSecret())
		if err != nil {
			return nil, fmt.Errorf("encrypt client secret: %w", err)
		}
		record["kind"] = kindClientInfo
		record["client_id"] = credential.Client.GetClientId()
		record["client_secret_encrypted"] = secretEnc
		record["client_secret_expires_at"] = utcTimePtr(credential.Client.GetClientSecretExpiresAt())
	case credential.Opaque != nil:
		fields, err := json.Marshal(credential.Opaque.GetFields())
		if err != nil {
			return nil, fmt.Errorf("encode opaque fields: %w", err)
		}
		fieldsEnc, err := s.encryptor.Encrypt(string(fields))
		if err != nil {
			return nil, fmt.Errorf("encrypt opaque fields: %w", err)
		}
		record["kind"] = kindOpaque
		record["fields_encrypted"] = fieldsEnc
	}
	return record, nil
}

func (s *store) recordToCredential(record gestalt.Record) (*gestalt.ExternalCredential, error) {
	credential := &gestalt.ExternalCredential{
		ID:           recordString(record, "id"),
		Subject:      strings.TrimSpace(recordString(record, "subject")),
		Audience:     recordString(record, "audience"),
		Qualifier:    recordString(record, "qualifier"),
		MetadataJSON: recordString(record, "metadata_json"),
		CreatedAt:    utcTimePtr(recordTimePtr(record, "created_at")),
		UpdatedAt:    utcTimePtr(recordTimePtr(record, "updated_at")),
	}
	switch kind := recordString(record, "kind"); kind {
	case kindGrant:
		accessToken, refreshToken, err := s.encryptor.DecryptTokenPair(
			recordString(record, "access_token_encrypted"),
			recordString(record, "refresh_token_encrypted"),
		)
		if err != nil {
			return nil, fmt.Errorf("decrypt grant tokens: %w", err)
		}
		credential.Grant = &gestalt.ExternalCredentialGrant{
			AccessToken:       accessToken,
			RefreshToken:      refreshToken,
			Scope:             recordString(record, "scope"),
			ExpiresAt:         utcTimePtr(recordTimePtr(record, "expires_at")),
			LastRefreshedAt:   utcTimePtr(recordTimePtr(record, "last_refreshed_at")),
			RefreshErrorCount: int32(recordInt(record, "refresh_error_count")),
		}
	case kindClientInfo:
		secret, err := s.encryptor.Decrypt(recordString(record, "client_secret_encrypted"))
		if err != nil {
			return nil, fmt.Errorf("decrypt client secret: %w", err)
		}
		credential.Client = &gestalt.ExternalCredentialClientInfo{
			ClientID:              recordString(record, "client_id"),
			ClientSecret:          secret,
			ClientSecretExpiresAt: utcTimePtr(recordTimePtr(record, "client_secret_expires_at")),
		}
	case kindOpaque:
		raw, err := s.encryptor.Decrypt(recordString(record, "fields_encrypted"))
		if err != nil {
			return nil, fmt.Errorf("decrypt opaque fields: %w", err)
		}
		fields := map[string]string{}
		if raw != "" {
			if err := json.Unmarshal([]byte(raw), &fields); err != nil {
				return nil, fmt.Errorf("decode opaque fields: %w", err)
			}
		}
		credential.Opaque = &gestalt.ExternalCredentialOpaque{Fields: fields}
	default:
		return nil, fmt.Errorf("external credential %q has unknown kind %q", credential.GetId(), kind)
	}
	return credential, nil
}

func normalizeCredential(credential *gestalt.ExternalCredential) (*gestalt.ExternalCredential, error) {
	if credential == nil {
		return nil, fmt.Errorf("credential is required")
	}
	clone := *credential
	clone.ID = strings.TrimSpace(clone.GetId())
	clone.Subject = strings.TrimSpace(clone.GetSubject())
	clone.Audience = strings.TrimSpace(clone.GetAudience())
	clone.Qualifier = strings.TrimSpace(clone.GetQualifier())
	if clone.Subject == "" {
		return nil, fmt.Errorf("credential subject is required")
	}
	if clone.Audience == "" {
		return nil, fmt.Errorf("credential audience is required")
	}
	set := 0
	for _, present := range []bool{clone.Grant != nil, clone.Client != nil, clone.Opaque != nil} {
		if present {
			set++
		}
	}
	if set != 1 {
		return nil, fmt.Errorf("credential requires exactly one of grant, client, opaque")
	}
	return &clone, nil
}

func recordString(record gestalt.Record, key string) string {
	value, _ := record[key].(string)
	return value
}

func recordInt(record gestalt.Record, key string) int {
	switch value := record[key].(type) {
	case int:
		return value
	case int8:
		return int(value)
	case int16:
		return int(value)
	case int32:
		return int(value)
	case int64:
		return int(value)
	case uint:
		return int(value)
	case uint8:
		return int(value)
	case uint16:
		return int(value)
	case uint32:
		return int(value)
	case uint64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}

func recordTime(record gestalt.Record, key string) time.Time {
	value, ok := record[key]
	if !ok || value == nil {
		return time.Time{}
	}

	switch raw := value.(type) {
	case time.Time:
		return raw
	case *time.Time:
		if raw == nil {
			return time.Time{}
		}
		return *raw
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return time.Time{}
		}
		return parsed
	default:
		return time.Time{}
	}
}

func recordTimePtr(record gestalt.Record, key string) *time.Time {
	value := recordTime(record, key)
	if value.IsZero() {
		return nil
	}
	return &value
}

func utcTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}
