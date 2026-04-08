package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	datastorecollections "github.com/valon-technologies/gestalt-providers/datastore/internal/collections"
	"github.com/valon-technologies/gestalt-providers/datastore/internal/sealcodec"
	"github.com/valon-technologies/gestalt-providers/datastore/internal/versioning"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const providerName = "mongodb"

var supportedVersions = []string{"7", "8"}

type Store struct {
	client *mongo.Client
	db     *mongo.Database
}

type userDoc struct {
	ID          string    `bson:"_id"`
	Email       string    `bson:"email"`
	DisplayName string    `bson:"display_name"`
	CreatedAt   time.Time `bson:"created_at"`
	UpdatedAt   time.Time `bson:"updated_at"`
}

type integrationTokenDoc struct {
	ID                    string     `bson:"_id"`
	UserID                string     `bson:"user_id"`
	Integration           string     `bson:"integration"`
	Connection            string     `bson:"connection"`
	Instance              string     `bson:"instance"`
	AccessTokenEncrypted  string     `bson:"access_token_encrypted"`
	RefreshTokenEncrypted string     `bson:"refresh_token_encrypted"`
	Scopes                string     `bson:"scopes"`
	ExpiresAt             *time.Time `bson:"expires_at"`
	LastRefreshedAt       *time.Time `bson:"last_refreshed_at"`
	RefreshErrorCount     int32      `bson:"refresh_error_count"`
	MetadataJSON          string     `bson:"metadata_json"`
	CreatedAt             time.Time  `bson:"created_at"`
	UpdatedAt             time.Time  `bson:"updated_at"`
}

type apiTokenDoc struct {
	ID          string     `bson:"_id"`
	UserID      string     `bson:"user_id"`
	Name        string     `bson:"name"`
	HashedToken string     `bson:"hashed_token"`
	Scopes      string     `bson:"scopes"`
	ExpiresAt   *time.Time `bson:"expires_at"`
	CreatedAt   time.Time  `bson:"created_at"`
	UpdatedAt   time.Time  `bson:"updated_at"`
}

func NewStore(uri, database string) (*Store, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongodb: connecting: %w", err)
	}

	if err := client.Ping(context.Background(), nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongodb: ping: %w", err)
	}

	if _, err := resolveVersion(context.Background(), client, ""); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}

	return &Store{
		client: client,
		db:     client.Database(database),
	}, nil
}

func resolveVersion(ctx context.Context, client *mongo.Client, requested string) (string, error) {
	return versioning.Resolve(ctx, providerName, requested, supportedVersions, func(ctx context.Context) (string, string, error) {
		var buildInfo struct {
			Version string `bson:"version"`
		}
		if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err != nil {
			return "", "", fmt.Errorf("%s: detecting version: %w", providerName, err)
		}

		var major int
		if _, err := fmt.Sscanf(buildInfo.Version, "%d", &major); err != nil {
			return "", buildInfo.Version, fmt.Errorf("%s: parsing server version %q: %w", providerName, buildInfo.Version, err)
		}
		return fmt.Sprintf("%d", major), buildInfo.Version, nil
	})
}

func (s *Store) HealthCheck(ctx context.Context) error {
	return s.client.Ping(ctx, nil)
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.Collection(datastorecollections.UsersCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "email", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("mongodb: creating users email index: %w", err)
	}

	_, err = s.db.Collection(datastorecollections.IntegrationTokensCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "user_id", Value: 1}, {Key: "integration", Value: 1}, {Key: "connection", Value: 1}, {Key: "instance", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("mongodb: creating integration_tokens compound index: %w", err)
	}

	_, err = s.db.Collection(datastorecollections.APITokensCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "hashed_token", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("mongodb: creating api_tokens hashed_token index: %w", err)
	}

	return nil
}

func (s *Store) Close() error {
	return s.client.Disconnect(context.Background())
}

func (s *Store) GetUser(ctx context.Context, id string) (*gestalt.StoredUser, error) {
	var doc userDoc
	err := s.db.Collection(datastorecollections.UsersCollection).FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("mongodb: getting user: %w", err)
	}
	return userFromDoc(&doc), nil
}

func (s *Store) FindOrCreateUser(ctx context.Context, email string) (*gestalt.StoredUser, error) {
	var doc userDoc
	err := s.db.Collection(datastorecollections.UsersCollection).FindOne(ctx, bson.M{"email": email}).Decode(&doc)
	if err == nil {
		return userFromDoc(&doc), nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("mongodb: querying user by email: %w", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	doc = userDoc{
		ID:        uuid.NewString(),
		Email:     email,
		CreatedAt: now,
		UpdatedAt: now,
	}

	_, err = s.db.Collection(datastorecollections.UsersCollection).InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			err = s.db.Collection(datastorecollections.UsersCollection).FindOne(ctx, bson.M{"email": email}).Decode(&doc)
			if err != nil {
				return nil, fmt.Errorf("mongodb: re-querying user after duplicate key: %w", err)
			}
			return userFromDoc(&doc), nil
		}
		return nil, fmt.Errorf("mongodb: inserting user: %w", err)
	}
	return userFromDoc(&doc), nil
}

func (s *Store) PutIntegrationToken(ctx context.Context, token *gestalt.StoredIntegrationToken) error {
	paramsJSON, err := connectionParamsToJSON(token.ConnectionParams)
	if err != nil {
		return fmt.Errorf("mongodb: encode connection params: %w", err)
	}

	filter := bson.M{
		"user_id":     token.UserID,
		"integration": token.Integration,
		"connection":  token.Connection,
		"instance":    token.Instance,
	}
	update := bson.M{
		"$set": bson.M{
			"access_token_encrypted":  sealcodec.Encode(token.AccessTokenSealed),
			"refresh_token_encrypted": sealcodec.Encode(token.RefreshTokenSealed),
			"scopes":                  token.Scopes,
			"expires_at":              token.ExpiresAt,
			"last_refreshed_at":       token.LastRefreshedAt,
			"refresh_error_count":     token.RefreshErrorCount,
			"metadata_json":           paramsJSON,
			"updated_at":              token.UpdatedAt,
		},
		"$setOnInsert": bson.M{
			"_id":         token.ID,
			"user_id":     token.UserID,
			"integration": token.Integration,
			"connection":  token.Connection,
			"instance":    token.Instance,
			"created_at":  token.CreatedAt,
		},
	}
	_, err = s.db.Collection(datastorecollections.IntegrationTokensCollection).UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("mongodb: upserting integration token: %w", err)
	}
	return nil
}

func (s *Store) GetIntegrationToken(ctx context.Context, userID, integration, connection, instance string) (*gestalt.StoredIntegrationToken, error) {
	var doc integrationTokenDoc
	err := s.db.Collection(datastorecollections.IntegrationTokensCollection).FindOne(ctx, bson.M{
		"user_id":     userID,
		"integration": integration,
		"connection":  connection,
		"instance":    instance,
	}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("mongodb: querying integration token: %w", err)
	}
	return integrationTokenFromDoc(&doc)
}

func (s *Store) ListIntegrationTokens(ctx context.Context, userID, integration, connection string) ([]*gestalt.StoredIntegrationToken, error) {
	filter := bson.M{"user_id": userID}
	if integration != "" {
		filter["integration"] = integration
	}
	if connection != "" {
		filter["connection"] = connection
	}
	cursor, err := s.db.Collection(datastorecollections.IntegrationTokensCollection).Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("mongodb: listing integration tokens: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var out []*gestalt.StoredIntegrationToken
	for cursor.Next(ctx) {
		var doc integrationTokenDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("mongodb: decoding integration token: %w", err)
		}
		token, err := integrationTokenFromDoc(&doc)
		if err != nil {
			return nil, err
		}
		out = append(out, token)
	}
	return out, cursor.Err()
}

func (s *Store) DeleteIntegrationToken(ctx context.Context, id string) error {
	_, err := s.db.Collection(datastorecollections.IntegrationTokensCollection).DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("mongodb: deleting integration token: %w", err)
	}
	return nil
}

func (s *Store) PutAPIToken(ctx context.Context, token *gestalt.StoredAPIToken) error {
	doc := apiTokenDoc{
		ID:          token.ID,
		UserID:      token.UserID,
		Name:        token.Name,
		HashedToken: token.HashedToken,
		Scopes:      token.Scopes,
		ExpiresAt:   token.ExpiresAt,
		CreatedAt:   token.CreatedAt,
		UpdatedAt:   token.UpdatedAt,
	}
	_, err := s.db.Collection(datastorecollections.APITokensCollection).InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("mongodb: inserting api token: %w", err)
	}
	return nil
}

func (s *Store) GetAPITokenByHash(ctx context.Context, hashedToken string) (*gestalt.StoredAPIToken, error) {
	now := time.Now()
	filter := bson.M{
		"hashed_token": hashedToken,
		"$or": bson.A{
			bson.M{"expires_at": nil},
			bson.M{"expires_at": bson.M{"$gt": now}},
		},
	}
	var doc apiTokenDoc
	err := s.db.Collection(datastorecollections.APITokensCollection).FindOne(ctx, filter).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("mongodb: getting api token by hash: %w", err)
	}
	return apiTokenFromDoc(&doc), nil
}

func (s *Store) ListAPITokens(ctx context.Context, userID string) ([]*gestalt.StoredAPIToken, error) {
	cursor, err := s.db.Collection(datastorecollections.APITokensCollection).Find(ctx, bson.M{"user_id": userID})
	if err != nil {
		return nil, fmt.Errorf("mongodb: listing api tokens: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var out []*gestalt.StoredAPIToken
	for cursor.Next(ctx) {
		var doc apiTokenDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("mongodb: decoding api token: %w", err)
		}
		out = append(out, apiTokenFromDoc(&doc))
	}
	return out, cursor.Err()
}

func (s *Store) RevokeAPIToken(ctx context.Context, userID, id string) error {
	result, err := s.db.Collection(datastorecollections.APITokensCollection).DeleteOne(ctx, bson.M{"_id": id, "user_id": userID})
	if err != nil {
		return fmt.Errorf("mongodb: revoking api token: %w", err)
	}
	if result.DeletedCount == 0 {
		return status.Errorf(codes.NotFound, "api token %s for user %s not found", id, userID)
	}
	return nil
}

func (s *Store) RevokeAllAPITokens(ctx context.Context, userID string) (int64, error) {
	result, err := s.db.Collection(datastorecollections.APITokensCollection).DeleteMany(ctx, bson.M{"user_id": userID})
	if err != nil {
		return 0, fmt.Errorf("mongodb: revoking all api tokens: %w", err)
	}
	return result.DeletedCount, nil
}

func userFromDoc(doc *userDoc) *gestalt.StoredUser {
	return &gestalt.StoredUser{
		ID:          doc.ID,
		Email:       doc.Email,
		DisplayName: doc.DisplayName,
		CreatedAt:   doc.CreatedAt,
		UpdatedAt:   doc.UpdatedAt,
	}
}

func integrationTokenFromDoc(doc *integrationTokenDoc) (*gestalt.StoredIntegrationToken, error) {
	params, err := connectionParamsFromJSON(doc.MetadataJSON)
	if err != nil {
		return nil, fmt.Errorf("mongodb: decode connection params: %w", err)
	}
	accessTokenSealed, err := sealcodec.Decode(doc.AccessTokenEncrypted)
	if err != nil {
		return nil, fmt.Errorf("mongodb: decode access token: %w", err)
	}
	refreshTokenSealed, err := sealcodec.Decode(doc.RefreshTokenEncrypted)
	if err != nil {
		return nil, fmt.Errorf("mongodb: decode refresh token: %w", err)
	}
	return &gestalt.StoredIntegrationToken{
		ID:                 doc.ID,
		UserID:             doc.UserID,
		Integration:        doc.Integration,
		Connection:         doc.Connection,
		Instance:           doc.Instance,
		AccessTokenSealed:  accessTokenSealed,
		RefreshTokenSealed: refreshTokenSealed,
		Scopes:             doc.Scopes,
		ExpiresAt:          doc.ExpiresAt,
		LastRefreshedAt:    doc.LastRefreshedAt,
		RefreshErrorCount:  doc.RefreshErrorCount,
		ConnectionParams:   params,
		CreatedAt:          doc.CreatedAt,
		UpdatedAt:          doc.UpdatedAt,
	}, nil
}

func apiTokenFromDoc(doc *apiTokenDoc) *gestalt.StoredAPIToken {
	return &gestalt.StoredAPIToken{
		ID:          doc.ID,
		UserID:      doc.UserID,
		Name:        doc.Name,
		HashedToken: doc.HashedToken,
		Scopes:      doc.Scopes,
		ExpiresAt:   doc.ExpiresAt,
		CreatedAt:   doc.CreatedAt,
		UpdatedAt:   doc.UpdatedAt,
	}
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
