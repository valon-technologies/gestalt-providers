package externalcredentials

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	storeName                 = "external_credentials"
	indexBySubject            = "by_subject"
	indexBySubjectIntegration = "by_subject_integration"
	indexBySubjectConnection  = "by_subject_connection"
	indexByLookup             = "by_lookup"
)

var storeSchema = gestalt.ObjectStoreSchema{
	Indexes: []gestalt.IndexSchema{
		{Name: indexBySubject, KeyPath: []string{"subject_id"}},
		{Name: indexBySubjectIntegration, KeyPath: []string{"subject_id", "integration"}},
		{Name: indexBySubjectConnection, KeyPath: []string{"subject_id", "integration", "connection"}},
		{Name: indexByLookup, KeyPath: []string{"subject_id", "integration", "connection", "instance"}, Unique: true},
	},
	Columns: []gestalt.ColumnDef{
		{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
		{Name: "subject_id", Type: gestalt.TypeString, NotNull: true},
		{Name: "integration", Type: gestalt.TypeString, NotNull: true},
		{Name: "connection", Type: gestalt.TypeString, NotNull: true},
		{Name: "instance", Type: gestalt.TypeString},
		{Name: "access_token_encrypted", Type: gestalt.TypeString},
		{Name: "refresh_token_encrypted", Type: gestalt.TypeString},
		{Name: "scopes", Type: gestalt.TypeString},
		{Name: "expires_at", Type: gestalt.TypeTime},
		{Name: "last_refreshed_at", Type: gestalt.TypeTime},
		{Name: "refresh_error_count", Type: gestalt.TypeInt},
		{Name: "metadata_json", Type: gestalt.TypeString},
		{Name: "created_at", Type: gestalt.TypeTime},
		{Name: "updated_at", Type: gestalt.TypeTime},
	},
}

type store struct {
	client      *gestalt.IndexedDBClient
	credentials *gestalt.ObjectStoreClient
	encryptor   *aesgcmEncryptor
}

func openStore(ctx context.Context, cfg config) (*store, error) {
	var (
		client *gestalt.IndexedDBClient
		err    error
	)
	if cfg.IndexedDB == "" {
		client, err = gestalt.IndexedDB()
	} else {
		client, err = gestalt.IndexedDB(cfg.IndexedDB)
	}
	if err != nil {
		return nil, fmt.Errorf("connect indexeddb: %w", err)
	}

	encryptor, err := newEncryptorFromConfig(cfg.EncryptionKey)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("build encryptor: %w", err)
	}

	st := &store{
		client:      client,
		credentials: client.ObjectStore(storeName),
		encryptor:   encryptor,
	}
	if err := st.ensure(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return st, nil
}

func (s *store) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *store) ensure(ctx context.Context) error {
	if err := s.client.CreateObjectStore(ctx, storeName, storeSchema); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create object store %q: %w", storeName, err)
	}
	return nil
}

func (s *store) upsertCredential(ctx context.Context, credential *gestalt.ExternalCredential, preserveTimestamps bool, now time.Time) (*gestalt.ExternalCredential, error) {
	if credential == nil {
		return nil, fmt.Errorf("credential is required")
	}

	normalized := normalizeCredential(credential)
	accessEnc, refreshEnc, err := s.encryptor.EncryptTokenPair(normalized.GetAccessToken(), normalized.GetRefreshToken())
	if err != nil {
		return nil, fmt.Errorf("encrypt credential pair: %w", err)
	}
	if normalized.GetId() == "" {
		normalized.Id = uuid.NewString()
	}

	createdAt := credentialCreatedAt(normalized, now)
	updatedAt := credentialUpdatedAt(normalized, now, preserveTimestamps)
	record := gestalt.Record{
		"subject_id":              normalized.GetSubjectId(),
		"integration":             normalized.GetIntegration(),
		"connection":              normalized.GetConnection(),
		"instance":                normalized.GetInstance(),
		"access_token_encrypted":  accessEnc,
		"refresh_token_encrypted": refreshEnc,
		"scopes":                  normalized.GetScopes(),
		"expires_at":              timeFromProto(normalized.GetExpiresAt()),
		"last_refreshed_at":       timeFromProto(normalized.GetLastRefreshedAt()),
		"refresh_error_count":     normalized.GetRefreshErrorCount(),
		"metadata_json":           normalized.GetMetadataJson(),
		"updated_at":              updatedAt,
	}

	existing, err := s.credentialRecord(ctx, normalized.GetSubjectId(), normalized.GetIntegration(), normalized.GetConnection(), normalized.GetInstance())
	switch {
	case err == nil:
		normalized.Id = recordString(existing, "id")
		record["id"] = normalized.GetId()
		existingCreatedAt := recordTime(existing, "created_at")
		if preserveTimestamps && normalized.GetCreatedAt() != nil {
			existingCreatedAt = normalized.GetCreatedAt().AsTime().UTC()
		}
		if existingCreatedAt.IsZero() {
			existingCreatedAt = createdAt
		}
		record["created_at"] = existingCreatedAt
		if err := s.credentials.Put(ctx, record); err != nil {
			return nil, fmt.Errorf("update external credential: %w", err)
		}
	case errors.Is(err, gestalt.ErrExternalCredentialNotFound):
		record["id"] = normalized.GetId()
		record["created_at"] = createdAt
		if err := s.credentials.Add(ctx, record); err != nil {
			return nil, fmt.Errorf("create external credential: %w", err)
		}
	default:
		return nil, fmt.Errorf("check existing external credential: %w", err)
	}

	if err := s.deleteDuplicateLookupRecords(ctx, normalized.GetId(), normalized.GetSubjectId(), normalized.GetIntegration(), normalized.GetConnection(), normalized.GetInstance()); err != nil {
		return nil, err
	}
	return s.getCredential(ctx, normalized.GetSubjectId(), normalized.GetIntegration(), normalized.GetConnection(), normalized.GetInstance())
}

func (s *store) getCredential(ctx context.Context, subjectID, integration, connection, instance string) (*gestalt.ExternalCredential, error) {
	record, err := s.credentialRecord(ctx, subjectID, integration, connection, instance)
	if err != nil {
		return nil, err
	}
	return s.recordToCredential(record)
}

func (s *store) listCredentials(ctx context.Context, subjectID, integration, connection, instance string) ([]*gestalt.ExternalCredential, error) {
	var (
		records []gestalt.Record
		err     error
	)
	switch {
	case integration != "" && connection != "":
		records, err = s.listCredentialRecords(ctx, indexBySubjectConnection, subjectID, integration, connection)
	case integration != "":
		records, err = s.listCredentialRecords(ctx, indexBySubjectIntegration, subjectID, integration)
	default:
		records, err = s.listCredentialRecords(ctx, indexBySubject, subjectID)
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
		if integration != "" && credential.GetIntegration() != integration {
			continue
		}
		if connection != "" && credential.GetConnection() != connection {
			continue
		}
		if instance != "" && credential.GetInstance() != instance {
			continue
		}
		credentials = append(credentials, credential)
	}
	return credentials, nil
}

func (s *store) deleteCredential(ctx context.Context, id string) error {
	record, err := s.credentials.Get(ctx, id)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get external credential by id: %w", err)
	}

	records, err := s.credentials.Index(indexByLookup).GetAll(ctx, nil,
		credentialRecordSubjectID(record),
		recordString(record, "integration"),
		recordString(record, "connection"),
		recordString(record, "instance"),
	)
	if err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return fmt.Errorf("list external credentials for delete: %w", err)
	}

	for _, duplicate := range records {
		duplicateID := recordString(duplicate, "id")
		if duplicateID == "" {
			continue
		}
		if err := s.credentials.Delete(ctx, duplicateID); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
			return fmt.Errorf("delete external credential %q: %w", duplicateID, err)
		}
	}
	return nil
}

func (s *store) credentialRecord(ctx context.Context, subjectID, integration, connection, instance string) (gestalt.Record, error) {
	records, err := s.listCredentialRecords(ctx, indexByLookup, subjectID, integration, connection, instance)
	if err != nil {
		return nil, fmt.Errorf("get external credential: %w", err)
	}
	if len(records) == 0 {
		return nil, gestalt.ErrExternalCredentialNotFound
	}
	return records[0], nil
}

func (s *store) listCredentialRecords(ctx context.Context, indexName string, keys ...any) ([]gestalt.Record, error) {
	records, err := s.credentials.Index(indexName).GetAll(ctx, nil, keys...)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return dedupeCredentialRecords(records), nil
}

func (s *store) deleteDuplicateLookupRecords(ctx context.Context, keepID, subjectID, integration, connection, instance string) error {
	records, err := s.credentials.Index(indexByLookup).GetAll(ctx, nil, subjectID, integration, connection, instance)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("list duplicate external credentials: %w", err)
	}
	for _, record := range records {
		id := recordString(record, "id")
		if id == "" || id == keepID {
			continue
		}
		if err := s.credentials.Delete(ctx, id); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
			return fmt.Errorf("delete duplicate external credential %q: %w", id, err)
		}
	}
	return nil
}

func (s *store) recordToCredential(record gestalt.Record) (*gestalt.ExternalCredential, error) {
	accessToken, refreshToken, err := s.encryptor.DecryptTokenPair(
		recordString(record, "access_token_encrypted"),
		recordString(record, "refresh_token_encrypted"),
	)
	if err != nil {
		return nil, fmt.Errorf("decrypt credential pair: %w", err)
	}

	return &gestalt.ExternalCredential{
		Id:                recordString(record, "id"),
		SubjectId:         credentialRecordSubjectID(record),
		Integration:       recordString(record, "integration"),
		Connection:        recordString(record, "connection"),
		Instance:          recordString(record, "instance"),
		AccessToken:       accessToken,
		RefreshToken:      refreshToken,
		Scopes:            recordString(record, "scopes"),
		ExpiresAt:         timeToProto(recordTimePtr(record, "expires_at")),
		LastRefreshedAt:   timeToProto(recordTimePtr(record, "last_refreshed_at")),
		RefreshErrorCount: int32(recordInt(record, "refresh_error_count")),
		MetadataJson:      recordString(record, "metadata_json"),
		CreatedAt:         timeToProto(recordTimePtr(record, "created_at")),
		UpdatedAt:         timeToProto(recordTimePtr(record, "updated_at")),
	}, nil
}

func normalizeCredential(credential *gestalt.ExternalCredential) *gestalt.ExternalCredential {
	if credential == nil {
		return nil
	}
	clone := *credential
	clone.Id = strings.TrimSpace(clone.GetId())
	clone.SubjectId = strings.TrimSpace(clone.GetSubjectId())
	clone.Integration = strings.TrimSpace(clone.GetIntegration())
	clone.Connection = strings.TrimSpace(clone.GetConnection())
	clone.Instance = strings.TrimSpace(clone.GetInstance())
	return &clone
}

func credentialCreatedAt(credential *gestalt.ExternalCredential, fallback time.Time) time.Time {
	if credential != nil && credential.GetCreatedAt() != nil {
		return credential.GetCreatedAt().AsTime().UTC()
	}
	return fallback.UTC()
}

func credentialUpdatedAt(credential *gestalt.ExternalCredential, fallback time.Time, preserve bool) time.Time {
	if preserve && credential != nil && credential.GetUpdatedAt() != nil {
		return credential.GetUpdatedAt().AsTime().UTC()
	}
	return fallback.UTC()
}

func credentialRecordSubjectID(record gestalt.Record) string {
	return strings.TrimSpace(recordString(record, "subject_id"))
}

func dedupeCredentialRecords(records []gestalt.Record) []gestalt.Record {
	if len(records) <= 1 {
		return records
	}

	bestByLookup := make(map[string]gestalt.Record, len(records))
	for _, record := range records {
		key := credentialLookupKey(record)
		best, ok := bestByLookup[key]
		if !ok || credentialRecordLess(record, best) {
			bestByLookup[key] = record
		}
	}

	out := make([]gestalt.Record, 0, len(bestByLookup))
	for _, record := range bestByLookup {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		return credentialRecordLess(out[i], out[j])
	})
	return out
}

func credentialLookupKey(record gestalt.Record) string {
	return credentialRecordSubjectID(record) + "\x00" +
		recordString(record, "integration") + "\x00" +
		recordString(record, "connection") + "\x00" +
		recordString(record, "instance")
}

func credentialRecordLess(left, right gestalt.Record) bool {
	leftUpdated := recordTime(left, "updated_at")
	rightUpdated := recordTime(right, "updated_at")
	if !leftUpdated.Equal(rightUpdated) {
		return leftUpdated.After(rightUpdated)
	}

	leftCreated := recordTime(left, "created_at")
	rightCreated := recordTime(right, "created_at")
	if !leftCreated.Equal(rightCreated) {
		return leftCreated.After(rightCreated)
	}

	return recordString(left, "id") < recordString(right, "id")
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

func timeToProto(value *time.Time) *timestamppb.Timestamp {
	if value == nil || value.IsZero() {
		return nil
	}
	return timestamppb.New(value.UTC())
}

func timeFromProto(value *timestamppb.Timestamp) *time.Time {
	if value == nil {
		return nil
	}
	asTime := value.AsTime().UTC()
	return &asTime
}
