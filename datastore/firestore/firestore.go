package firestore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	gcpfirestore "cloud.google.com/go/firestore"
	"github.com/google/uuid"
	datastorecollections "github.com/valon-technologies/gestalt-providers/datastore/internal/collections"
	"github.com/valon-technologies/gestalt-providers/datastore/internal/sealcodec"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	usersByEmailCollection         = "users_by_email"
	integrationTokenKeysCollection = "integration_token_keys"
	apiTokensByHashCollection      = "api_tokens_by_hash"
	findUserTxnMaxAttempts         = 5
)

type Store struct {
	client *gcpfirestore.Client
}

type userDoc struct {
	Email       string    `firestore:"email"`
	DisplayName string    `firestore:"display_name"`
	CreatedAt   time.Time `firestore:"created_at"`
	UpdatedAt   time.Time `firestore:"updated_at"`
}

type userLookupDoc struct {
	UserID string `firestore:"user_id"`
}

type integrationTokenDoc struct {
	UserID                string     `firestore:"user_id"`
	Integration           string     `firestore:"integration"`
	Connection            string     `firestore:"connection"`
	Instance              string     `firestore:"instance"`
	AccessTokenEncrypted  string     `firestore:"access_token_encrypted"`
	RefreshTokenEncrypted string     `firestore:"refresh_token_encrypted"`
	Scopes                string     `firestore:"scopes"`
	ExpiresAt             *time.Time `firestore:"expires_at"`
	LastRefreshedAt       *time.Time `firestore:"last_refreshed_at"`
	RefreshErrorCount     int32      `firestore:"refresh_error_count"`
	MetadataJSON          string     `firestore:"metadata_json"`
	CreatedAt             time.Time  `firestore:"created_at"`
	UpdatedAt             time.Time  `firestore:"updated_at"`
}

type integrationTokenLookupDoc struct {
	TokenID string `firestore:"token_id"`
}

type apiTokenDoc struct {
	UserID      string     `firestore:"user_id"`
	Name        string     `firestore:"name"`
	HashedToken string     `firestore:"hashed_token"`
	Scopes      string     `firestore:"scopes"`
	ExpiresAt   *time.Time `firestore:"expires_at"`
	CreatedAt   time.Time  `firestore:"created_at"`
	UpdatedAt   time.Time  `firestore:"updated_at"`
}

type apiTokenHashLookupDoc struct {
	TokenID string `firestore:"token_id"`
}

func NewStore(projectID, database string) (*Store, error) {
	ctx := context.Background()
	var (
		client *gcpfirestore.Client
		err    error
	)
	if database != "" {
		client, err = gcpfirestore.NewClientWithDatabase(ctx, projectID, database)
	} else {
		client, err = gcpfirestore.NewClient(ctx, projectID)
	}
	if err != nil {
		return nil, fmt.Errorf("firestore: creating client: %w", err)
	}
	return &Store{client: client}, nil
}

func (s *Store) HealthCheck(ctx context.Context) error {
	iter := s.client.Collection(datastorecollections.UsersCollection).Limit(1).Documents(ctx)
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return nil
	}
	return err
}

func (s *Store) Migrate(context.Context) error {
	return nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) GetUser(ctx context.Context, id string) (*gestalt.StoredUser, error) {
	snap, err := s.client.Collection(datastorecollections.UsersCollection).Doc(id).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("firestore: getting user: %w", err)
	}
	return snapToUser(snap)
}

func (s *Store) FindOrCreateUser(ctx context.Context, email string) (*gestalt.StoredUser, error) {
	user, err := s.findUserByEmail(ctx, email)
	if err != nil {
		return nil, err
	}
	if user != nil {
		return user, nil
	}

	now := time.Now().UTC().Truncate(time.Second)
	id := uuid.NewString()
	doc := userDoc{
		Email:     email,
		CreatedAt: now,
		UpdatedAt: now,
	}

	lookupRef := s.client.Collection(usersByEmailCollection).Doc(email)
	userRef := s.client.Collection(datastorecollections.UsersCollection).Doc(id)

	var created bool
	for attempt := range findUserTxnMaxAttempts {
		created = false
		err = s.client.RunTransaction(ctx, func(_ context.Context, tx *gcpfirestore.Transaction) error {
			snap, err := tx.Get(lookupRef)
			if err != nil && status.Code(err) != codes.NotFound {
				return fmt.Errorf("checking email lookup: %w", err)
			}
			if snap != nil && snap.Exists() {
				created = false
				return nil
			}
			if err := tx.Create(lookupRef, userLookupDoc{UserID: id}); err != nil {
				return err
			}
			created = true
			return tx.Create(userRef, doc)
		})
		if err == nil {
			break
		}
		if !isRetryableCreateUserError(err) || attempt == findUserTxnMaxAttempts-1 {
			break
		}
		if user, lookupErr := s.findUserByEmail(ctx, email); lookupErr == nil && user != nil {
			return user, nil
		}
		if sleepErr := sleepContext(ctx, time.Duration(attempt+1)*50*time.Millisecond); sleepErr != nil {
			return nil, sleepErr
		}
	}
	if err != nil {
		user, lookupErr := s.findUserByEmail(ctx, email)
		if lookupErr != nil {
			return nil, fmt.Errorf("firestore: re-querying user after conflict: %w", lookupErr)
		}
		if user != nil {
			return user, nil
		}
		return nil, fmt.Errorf("firestore: creating user: %w", err)
	}
	if !created {
		user, err = s.findUserByEmail(ctx, email)
		if err != nil {
			return nil, fmt.Errorf("firestore: querying existing user: %w", err)
		}
		if user == nil {
			return nil, fmt.Errorf("firestore: email lookup exists but user not found")
		}
		return user, nil
	}

	return &gestalt.StoredUser{
		Id:        id,
		Email:     email,
		CreatedAt: timestamppb.New(now),
		UpdatedAt: timestamppb.New(now),
	}, nil
}

func (s *Store) findUserByEmail(ctx context.Context, email string) (*gestalt.StoredUser, error) {
	lookupSnap, err := s.client.Collection(usersByEmailCollection).Doc(email).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("firestore: getting user email lookup: %w", err)
	}

	var lookup userLookupDoc
	if err := lookupSnap.DataTo(&lookup); err != nil {
		return nil, fmt.Errorf("firestore: unmarshalling user email lookup: %w", err)
	}

	return s.GetUser(ctx, lookup.UserID)
}

func snapToUser(snap *gcpfirestore.DocumentSnapshot) (*gestalt.StoredUser, error) {
	var doc userDoc
	if err := snap.DataTo(&doc); err != nil {
		return nil, fmt.Errorf("firestore: unmarshalling user: %w", err)
	}
	return &gestalt.StoredUser{
		Id:          snap.Ref.ID,
		Email:       doc.Email,
		DisplayName: doc.DisplayName,
		CreatedAt:   timestamppb.New(doc.CreatedAt),
		UpdatedAt:   timestamppb.New(doc.UpdatedAt),
	}, nil
}

func (s *Store) PutIntegrationToken(ctx context.Context, token *gestalt.StoredIntegrationToken) error {
	paramsJSON, err := connectionParamsToJSON(token.ConnectionParams)
	if err != nil {
		return fmt.Errorf("firestore: encode connection params: %w", err)
	}

	var expiresAt *time.Time
	if token.ExpiresAt != nil {
		t := token.ExpiresAt.AsTime()
		expiresAt = &t
	}
	var lastRefreshedAt *time.Time
	if token.LastRefreshedAt != nil {
		t := token.LastRefreshedAt.AsTime()
		lastRefreshedAt = &t
	}
	doc := integrationTokenDoc{
		UserID:                token.UserId,
		Integration:           token.Integration,
		Connection:            token.Connection,
		Instance:              token.Instance,
		AccessTokenEncrypted:  sealcodec.Encode(token.AccessTokenSealed),
		RefreshTokenEncrypted: sealcodec.Encode(token.RefreshTokenSealed),
		Scopes:                token.Scopes,
		ExpiresAt:             expiresAt,
		LastRefreshedAt:       lastRefreshedAt,
		RefreshErrorCount:     token.RefreshErrorCount,
		MetadataJSON:          paramsJSON,
		CreatedAt:             token.CreatedAt.AsTime(),
		UpdatedAt:             token.UpdatedAt.AsTime(),
	}

	lookupRef := s.client.Collection(integrationTokenKeysCollection).Doc(
		firestoreDocKey(token.UserId, token.Integration, token.Connection, token.Instance),
	)
	tokenRef := s.client.Collection(datastorecollections.IntegrationTokensCollection).Doc(token.Id)

	return s.client.RunTransaction(ctx, func(_ context.Context, tx *gcpfirestore.Transaction) error {
		existingTokenSnap, err := tx.Get(tokenRef)
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("getting existing token by id: %w", err)
		}
		var oldLookupRef *gcpfirestore.DocumentRef
		if err == nil && existingTokenSnap.Exists() {
			var existing integrationTokenDoc
			if err := existingTokenSnap.DataTo(&existing); err != nil {
				return fmt.Errorf("unmarshalling existing token by id: %w", err)
			}
			oldLookupRef = s.client.Collection(integrationTokenKeysCollection).Doc(
				firestoreDocKey(existing.UserID, existing.Integration, existing.Connection, existing.Instance),
			)
		}

		lookupSnap, err := tx.Get(lookupRef)
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("getting token lookup: %w", err)
		}
		if err == nil && lookupSnap.Exists() {
			var lookup integrationTokenLookupDoc
			if err := lookupSnap.DataTo(&lookup); err != nil {
				return fmt.Errorf("unmarshalling token lookup: %w", err)
			}
			if lookup.TokenID != "" && lookup.TokenID != token.Id {
				if err := tx.Delete(s.client.Collection(datastorecollections.IntegrationTokensCollection).Doc(lookup.TokenID)); err != nil {
					return fmt.Errorf("deleting stale integration token: %w", err)
				}
			}
		}

		if oldLookupRef != nil && oldLookupRef.ID != lookupRef.ID {
			if err := tx.Delete(oldLookupRef); err != nil {
				return fmt.Errorf("deleting stale token lookup: %w", err)
			}
		}
		if err := tx.Set(lookupRef, integrationTokenLookupDoc{TokenID: token.Id}); err != nil {
			return fmt.Errorf("storing token lookup: %w", err)
		}
		return tx.Set(tokenRef, doc)
	})
}

func (s *Store) GetIntegrationToken(ctx context.Context, userID, integration, connection, instance string) (*gestalt.StoredIntegrationToken, error) {
	lookupSnap, err := s.client.Collection(integrationTokenKeysCollection).Doc(
		firestoreDocKey(userID, integration, connection, instance),
	).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("firestore: getting token lookup: %w", err)
	}

	var lookup integrationTokenLookupDoc
	if err := lookupSnap.DataTo(&lookup); err != nil {
		return nil, fmt.Errorf("firestore: unmarshalling token lookup: %w", err)
	}

	snap, err := s.client.Collection(datastorecollections.IntegrationTokensCollection).Doc(lookup.TokenID).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("firestore: getting token by id: %w", err)
	}
	return snapToIntegrationToken(snap)
}

func (s *Store) ListIntegrationTokens(ctx context.Context, userID, integration, connection string) ([]*gestalt.StoredIntegrationToken, error) {
	iter := s.client.Collection(datastorecollections.IntegrationTokensCollection).
		Where("user_id", "==", userID).
		Documents(ctx)
	defer iter.Stop()

	var tokens []*gestalt.StoredIntegrationToken
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("firestore: listing tokens: %w", err)
		}
		token, err := snapToIntegrationToken(snap)
		if err != nil {
			return nil, err
		}
		if integration != "" && token.Integration != integration {
			continue
		}
		if connection != "" && token.Connection != connection {
			continue
		}
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func (s *Store) DeleteIntegrationToken(ctx context.Context, id string) error {
	tokenRef := s.client.Collection(datastorecollections.IntegrationTokensCollection).Doc(id)
	snap, err := tokenRef.Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("firestore: getting token for delete: %w", err)
	}

	var doc integrationTokenDoc
	if err := snap.DataTo(&doc); err != nil {
		return fmt.Errorf("firestore: unmarshalling token for delete: %w", err)
	}

	lookupRef := s.client.Collection(integrationTokenKeysCollection).Doc(
		firestoreDocKey(doc.UserID, doc.Integration, doc.Connection, doc.Instance),
	)

	return s.client.RunTransaction(ctx, func(_ context.Context, tx *gcpfirestore.Transaction) error {
		lookupSnap, err := tx.Get(lookupRef)
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("getting token lookup for delete: %w", err)
		}
		if err == nil && lookupSnap.Exists() {
			var lookup integrationTokenLookupDoc
			if err := lookupSnap.DataTo(&lookup); err != nil {
				return fmt.Errorf("unmarshalling token lookup for delete: %w", err)
			}
			if lookup.TokenID == id {
				if err := tx.Delete(lookupRef); err != nil {
					return fmt.Errorf("deleting token lookup: %w", err)
				}
			}
		}
		return tx.Delete(tokenRef)
	})
}

func snapToIntegrationToken(snap *gcpfirestore.DocumentSnapshot) (*gestalt.StoredIntegrationToken, error) {
	var doc integrationTokenDoc
	if err := snap.DataTo(&doc); err != nil {
		return nil, fmt.Errorf("firestore: unmarshalling integration token: %w", err)
	}
	params, err := connectionParamsFromJSON(doc.MetadataJSON)
	if err != nil {
		return nil, fmt.Errorf("firestore: decode connection params: %w", err)
	}
	accessTokenSealed, err := sealcodec.Decode(doc.AccessTokenEncrypted)
	if err != nil {
		return nil, fmt.Errorf("firestore: decode access token: %w", err)
	}
	refreshTokenSealed, err := sealcodec.Decode(doc.RefreshTokenEncrypted)
	if err != nil {
		return nil, fmt.Errorf("firestore: decode refresh token: %w", err)
	}
	var expiresAt *timestamppb.Timestamp
	if doc.ExpiresAt != nil {
		expiresAt = timestamppb.New(*doc.ExpiresAt)
	}
	var lastRefreshedAt *timestamppb.Timestamp
	if doc.LastRefreshedAt != nil {
		lastRefreshedAt = timestamppb.New(*doc.LastRefreshedAt)
	}
	return &gestalt.StoredIntegrationToken{
		Id:                 snap.Ref.ID,
		UserId:             doc.UserID,
		Integration:        doc.Integration,
		Connection:         doc.Connection,
		Instance:           doc.Instance,
		AccessTokenSealed:  accessTokenSealed,
		RefreshTokenSealed: refreshTokenSealed,
		Scopes:             doc.Scopes,
		ExpiresAt:          expiresAt,
		LastRefreshedAt:    lastRefreshedAt,
		RefreshErrorCount:  doc.RefreshErrorCount,
		ConnectionParams:   params,
		CreatedAt:          timestamppb.New(doc.CreatedAt),
		UpdatedAt:          timestamppb.New(doc.UpdatedAt),
	}, nil
}

func (s *Store) PutAPIToken(ctx context.Context, token *gestalt.StoredAPIToken) error {
	var expiresAt *time.Time
	if token.ExpiresAt != nil {
		t := token.ExpiresAt.AsTime()
		expiresAt = &t
	}
	doc := apiTokenDoc{
		UserID:      token.UserId,
		Name:        token.Name,
		HashedToken: token.HashedToken,
		Scopes:      token.Scopes,
		ExpiresAt:   expiresAt,
		CreatedAt:   token.CreatedAt.AsTime(),
		UpdatedAt:   token.UpdatedAt.AsTime(),
	}

	hashRef := s.client.Collection(apiTokensByHashCollection).Doc(firestoreDocKey(token.HashedToken))
	tokenRef := s.client.Collection(datastorecollections.APITokensCollection).Doc(token.Id)

	return s.client.RunTransaction(ctx, func(_ context.Context, tx *gcpfirestore.Transaction) error {
		existingTokenSnap, err := tx.Get(tokenRef)
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("getting existing api token by id: %w", err)
		}
		var oldHashRef *gcpfirestore.DocumentRef
		if err == nil && existingTokenSnap.Exists() {
			var existing apiTokenDoc
			if err := existingTokenSnap.DataTo(&existing); err != nil {
				return fmt.Errorf("unmarshalling existing api token by id: %w", err)
			}
			oldHashRef = s.client.Collection(apiTokensByHashCollection).Doc(firestoreDocKey(existing.HashedToken))
		}

		hashSnap, err := tx.Get(hashRef)
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("getting api token hash lookup: %w", err)
		}
		if err == nil && hashSnap.Exists() {
			var lookup apiTokenHashLookupDoc
			if err := hashSnap.DataTo(&lookup); err != nil {
				return fmt.Errorf("unmarshalling api token hash lookup: %w", err)
			}
			if lookup.TokenID != "" && lookup.TokenID != token.Id {
				return fmt.Errorf("firestore: hashed token already exists")
			}
		}

		if oldHashRef != nil && oldHashRef.ID != hashRef.ID {
			if err := tx.Delete(oldHashRef); err != nil {
				return fmt.Errorf("deleting stale api token hash lookup: %w", err)
			}
		}
		if err := tx.Set(hashRef, apiTokenHashLookupDoc{TokenID: token.Id}); err != nil {
			return fmt.Errorf("storing api token hash lookup: %w", err)
		}
		return tx.Set(tokenRef, doc)
	})
}

func (s *Store) GetAPITokenByHash(ctx context.Context, hashedToken string) (*gestalt.StoredAPIToken, error) {
	lookupSnap, err := s.client.Collection(apiTokensByHashCollection).Doc(firestoreDocKey(hashedToken)).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("firestore: getting api token hash lookup: %w", err)
	}

	var lookup apiTokenHashLookupDoc
	if err := lookupSnap.DataTo(&lookup); err != nil {
		return nil, fmt.Errorf("firestore: unmarshalling api token hash lookup: %w", err)
	}

	snap, err := s.client.Collection(datastorecollections.APITokensCollection).Doc(lookup.TokenID).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("firestore: getting api token by id: %w", err)
	}

	token, err := snapToAPIToken(snap)
	if err != nil {
		return nil, err
	}
	if token.ExpiresAt != nil && time.Now().After(token.ExpiresAt.AsTime()) {
		return nil, nil
	}
	return token, nil
}

func (s *Store) ListAPITokens(ctx context.Context, userID string) ([]*gestalt.StoredAPIToken, error) {
	iter := s.client.Collection(datastorecollections.APITokensCollection).
		Where("user_id", "==", userID).
		Documents(ctx)
	defer iter.Stop()

	var tokens []*gestalt.StoredAPIToken
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("firestore: listing api tokens: %w", err)
		}
		token, err := snapToAPIToken(snap)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func (s *Store) RevokeAPIToken(ctx context.Context, userID, id string) error {
	tokenRef := s.client.Collection(datastorecollections.APITokensCollection).Doc(id)
	snap, err := tokenRef.Get(ctx)
	if status.Code(err) == codes.NotFound {
		return status.Errorf(codes.NotFound, "api token %s for user %s not found", id, userID)
	}
	if err != nil {
		return fmt.Errorf("firestore: revoking api token: %w", err)
	}
	var doc apiTokenDoc
	if err := snap.DataTo(&doc); err != nil {
		return fmt.Errorf("firestore: unmarshalling api token: %w", err)
	}
	if doc.UserID != userID {
		return status.Errorf(codes.NotFound, "api token %s for user %s not found", id, userID)
	}

	hashRef := s.client.Collection(apiTokensByHashCollection).Doc(firestoreDocKey(doc.HashedToken))
	return s.client.RunTransaction(ctx, func(_ context.Context, tx *gcpfirestore.Transaction) error {
		hashSnap, err := tx.Get(hashRef)
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("getting api token hash lookup for revoke: %w", err)
		}

		if err := tx.Delete(tokenRef); err != nil {
			return fmt.Errorf("deleting api token: %w", err)
		}
		if err == nil && hashSnap.Exists() {
			var lookup apiTokenHashLookupDoc
			if err := hashSnap.DataTo(&lookup); err != nil {
				return fmt.Errorf("unmarshalling api token hash lookup for revoke: %w", err)
			}
			if lookup.TokenID == id {
				if err := tx.Delete(hashRef); err != nil {
					return fmt.Errorf("deleting api token hash lookup: %w", err)
				}
			}
		}
		return nil
	})
}

func (s *Store) RevokeAllAPITokens(ctx context.Context, userID string) (int64, error) {
	iter := s.client.Collection(datastorecollections.APITokensCollection).
		Where("user_id", "==", userID).
		Documents(ctx)
	defer iter.Stop()

	bw := s.client.BulkWriter(ctx)
	var jobs []*gcpfirestore.BulkWriterJob
	var deleted int64
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("firestore: iterating api tokens for revoke-all: %w", err)
		}
		var doc apiTokenDoc
		if err := snap.DataTo(&doc); err != nil {
			return 0, fmt.Errorf("firestore: unmarshalling api token for revoke-all: %w", err)
		}
		job, err := bw.Delete(snap.Ref)
		if err != nil {
			return 0, fmt.Errorf("firestore: queuing delete for revoke-all: %w", err)
		}
		jobs = append(jobs, job)
		job, err = bw.Delete(s.client.Collection(apiTokensByHashCollection).Doc(firestoreDocKey(doc.HashedToken)))
		if err != nil {
			return 0, fmt.Errorf("firestore: queuing hash lookup delete for revoke-all: %w", err)
		}
		jobs = append(jobs, job)
		deleted++
	}
	bw.End()

	for _, job := range jobs {
		if _, err := job.Results(); err != nil {
			return 0, fmt.Errorf("firestore: deleting api token in revoke-all: %w", err)
		}
	}
	return deleted, nil
}

func snapToAPIToken(snap *gcpfirestore.DocumentSnapshot) (*gestalt.StoredAPIToken, error) {
	var doc apiTokenDoc
	if err := snap.DataTo(&doc); err != nil {
		return nil, fmt.Errorf("firestore: unmarshalling api token: %w", err)
	}
	var expiresAt *timestamppb.Timestamp
	if doc.ExpiresAt != nil {
		expiresAt = timestamppb.New(*doc.ExpiresAt)
	}
	return &gestalt.StoredAPIToken{
		Id:          snap.Ref.ID,
		UserId:      doc.UserID,
		Name:        doc.Name,
		HashedToken: doc.HashedToken,
		Scopes:      doc.Scopes,
		ExpiresAt:   expiresAt,
		CreatedAt:   timestamppb.New(doc.CreatedAt),
		UpdatedAt:   timestamppb.New(doc.UpdatedAt),
	}, nil
}

func isRetryableCreateUserError(err error) bool {
	return status.Code(err) == codes.Aborted
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func firestoreDocKey(parts ...string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(parts, "\x1f")))
}

func connectionParamsToJSON(values map[string]string) (string, error) {
	if len(values) == 0 {
		return "", nil
	}
	data, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func connectionParamsFromJSON(value string) (map[string]string, error) {
	if value == "" {
		return nil, nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		return nil, err
	}
	return out, nil
}
