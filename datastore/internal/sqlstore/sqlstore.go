package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/valon-technologies/gestalt-providers/datastore/internal/sealcodec"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Dialect interface {
	Placeholder(n int) string
	UpsertTokenSQL() string
	IsDuplicateKeyError(err error) bool
	NormalizeConnection(connection string) string
	DenormalizeConnection(connection string) string
}

type RegistrationDDLProvider interface {
	RegistrationDDL() string
}

type Store struct {
	DB      *sql.DB
	Dialect Dialect
}

func New(db *sql.DB, dialect Dialect) *Store {
	return &Store{DB: db, Dialect: dialect}
}

func (s *Store) RawDB() any      { return s.DB }
func (s *Store) RawDialect() any { return s.Dialect }

func (s *Store) HealthCheck(ctx context.Context) error {
	return s.DB.PingContext(ctx)
}

func (s *Store) Close() error {
	return s.DB.Close()
}

func (s *Store) ph(n int) string { return s.Dialect.Placeholder(n) }

type Scanner interface {
	Scan(dest ...any) error
}

func defaultTimestamps(createdAt, updatedAt *time.Time) {
	now := time.Now().UTC().Truncate(time.Second)
	if createdAt.IsZero() {
		*createdAt = now
	}
	if updatedAt.IsZero() {
		*updatedAt = now
	}
}

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
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

func scanUser(row Scanner) (gestalt.StoredUser, error) {
	var user gestalt.StoredUser
	var displayName sql.NullString
	if err := row.Scan(&user.ID, &user.Email, &displayName, &user.CreatedAt, &user.UpdatedAt); err != nil {
		return user, err
	}
	user.DisplayName = displayName.String
	return user, nil
}

func (s *Store) GetUser(ctx context.Context, id string) (*gestalt.StoredUser, error) {
	row := s.DB.QueryRowContext(ctx,
		"SELECT id, email, display_name, created_at, updated_at FROM users WHERE id = "+s.ph(1),
		id,
	)
	user, err := scanUser(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying user by id: %w", err)
	}
	return &user, nil
}

func (s *Store) FindOrCreateUser(ctx context.Context, email string) (*gestalt.StoredUser, error) {
	row := s.DB.QueryRowContext(ctx,
		"SELECT id, email, display_name, created_at, updated_at FROM users WHERE email = "+s.ph(1),
		email,
	)
	user, err := scanUser(row)
	if err == nil {
		return &user, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("querying user: %w", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	user = gestalt.StoredUser{
		ID:        uuid.NewString(),
		Email:     email,
		CreatedAt: now,
		UpdatedAt: now,
	}

	_, err = s.DB.ExecContext(ctx,
		"INSERT INTO users (id, email, display_name, created_at, updated_at) VALUES ("+
			s.ph(1)+", "+s.ph(2)+", "+s.ph(3)+", "+s.ph(4)+", "+s.ph(5)+")",
		user.ID, user.Email, user.DisplayName, user.CreatedAt, user.UpdatedAt,
	)
	if err != nil {
		if s.Dialect.IsDuplicateKeyError(err) {
			requery := s.DB.QueryRowContext(ctx,
				"SELECT id, email, display_name, created_at, updated_at FROM users WHERE email = "+s.ph(1),
				email,
			)
			user, err = scanUser(requery)
			if err != nil {
				return nil, fmt.Errorf("re-querying user after duplicate key: %w", err)
			}
			return &user, nil
		}
		return nil, fmt.Errorf("inserting user: %w", err)
	}
	return &user, nil
}

func (s *Store) scanIntegrationToken(row Scanner) (*gestalt.StoredIntegrationToken, error) {
	var token gestalt.StoredIntegrationToken
	var accessSealed, refreshSealed sql.NullString
	var scopes, paramsJSON sql.NullString
	var expiresAt, lastRefreshedAt sql.NullTime
	var err error

	if err := row.Scan(
		&token.ID,
		&token.UserID,
		&token.Integration,
		&token.Connection,
		&token.Instance,
		&accessSealed,
		&refreshSealed,
		&scopes,
		&expiresAt,
		&lastRefreshedAt,
		&token.RefreshErrorCount,
		&paramsJSON,
		&token.CreatedAt,
		&token.UpdatedAt,
	); err != nil {
		return nil, err
	}

	token.Connection = s.Dialect.DenormalizeConnection(token.Connection)
	token.AccessTokenSealed, err = sealcodec.Decode(accessSealed.String)
	if err != nil {
		return nil, fmt.Errorf("decode access token: %w", err)
	}
	token.RefreshTokenSealed, err = sealcodec.Decode(refreshSealed.String)
	if err != nil {
		return nil, fmt.Errorf("decode refresh token: %w", err)
	}
	token.Scopes = scopes.String
	if expiresAt.Valid {
		token.ExpiresAt = &expiresAt.Time
	}
	if lastRefreshedAt.Valid {
		token.LastRefreshedAt = &lastRefreshedAt.Time
	}
	params, err := connectionParamsFromJSON(paramsJSON.String)
	if err != nil {
		return nil, fmt.Errorf("decode connection params: %w", err)
	}
	token.ConnectionParams = params
	return &token, nil
}

func (s *Store) PutIntegrationToken(ctx context.Context, token *gestalt.StoredIntegrationToken) error {
	defaultTimestamps(&token.CreatedAt, &token.UpdatedAt)

	paramsJSON, err := connectionParamsToJSON(token.ConnectionParams)
	if err != nil {
		return fmt.Errorf("encode connection params: %w", err)
	}

	connection := s.Dialect.NormalizeConnection(token.Connection)
	_, err = s.DB.ExecContext(ctx, s.Dialect.UpsertTokenSQL(),
		token.ID,
		token.UserID,
		token.Integration,
		connection,
		token.Instance,
		sealcodec.Encode(token.AccessTokenSealed),
		sealcodec.Encode(token.RefreshTokenSealed),
		token.Scopes,
		nullableTime(token.ExpiresAt),
		nullableTime(token.LastRefreshedAt),
		token.RefreshErrorCount,
		paramsJSON,
		token.CreatedAt,
		token.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upserting integration token: %w", err)
	}
	return nil
}

func (s *Store) GetIntegrationToken(ctx context.Context, userID, integration, connection, instance string) (*gestalt.StoredIntegrationToken, error) {
	connection = s.Dialect.NormalizeConnection(connection)
	row := s.DB.QueryRowContext(ctx, `
		SELECT id, user_id, integration, connection, instance, access_token_encrypted, refresh_token_encrypted,
		       scopes, expires_at, last_refreshed_at, refresh_error_count, metadata_json, created_at, updated_at
		FROM integration_tokens
		WHERE user_id = `+s.ph(1)+` AND integration = `+s.ph(2)+` AND connection = `+s.ph(3)+` AND instance = `+s.ph(4),
		userID, integration, connection, instance,
	)
	token, err := s.scanIntegrationToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying token: %w", err)
	}
	return token, nil
}

func (s *Store) ListIntegrationTokens(ctx context.Context, userID, integration, connection string) ([]*gestalt.StoredIntegrationToken, error) {
	query := `
		SELECT id, user_id, integration, connection, instance, access_token_encrypted, refresh_token_encrypted,
		       scopes, expires_at, last_refreshed_at, refresh_error_count, metadata_json, created_at, updated_at
		FROM integration_tokens
		WHERE user_id = ` + s.ph(1)
	args := []any{userID}

	if integration != "" {
		query += ` AND integration = ` + s.ph(len(args)+1)
		args = append(args, integration)
	}
	if connection != "" {
		query += ` AND connection = ` + s.ph(len(args)+1)
		args = append(args, s.Dialect.NormalizeConnection(connection))
	}

	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("listing integration tokens: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*gestalt.StoredIntegrationToken
	for rows.Next() {
		token, err := s.scanIntegrationToken(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning integration token row: %w", err)
		}
		out = append(out, token)
	}
	return out, rows.Err()
}

func (s *Store) DeleteIntegrationToken(ctx context.Context, id string) error {
	_, err := s.DB.ExecContext(ctx, "DELETE FROM integration_tokens WHERE id = "+s.ph(1), id)
	if err != nil {
		return fmt.Errorf("deleting integration token: %w", err)
	}
	return nil
}

func scanAPIToken(row Scanner) (*gestalt.StoredAPIToken, error) {
	var token gestalt.StoredAPIToken
	var scopes sql.NullString
	var expiresAt sql.NullTime
	if err := row.Scan(&token.ID, &token.UserID, &token.Name, &token.HashedToken, &scopes, &expiresAt, &token.CreatedAt, &token.UpdatedAt); err != nil {
		return nil, err
	}
	token.Scopes = scopes.String
	if expiresAt.Valid {
		token.ExpiresAt = &expiresAt.Time
	}
	return &token, nil
}

func (s *Store) PutAPIToken(ctx context.Context, token *gestalt.StoredAPIToken) error {
	defaultTimestamps(&token.CreatedAt, &token.UpdatedAt)
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO api_tokens (id, user_id, name, hashed_token, scopes, expires_at, created_at, updated_at)
		VALUES (`+s.ph(1)+`, `+s.ph(2)+`, `+s.ph(3)+`, `+s.ph(4)+`, `+s.ph(5)+`, `+s.ph(6)+`, `+s.ph(7)+`, `+s.ph(8)+`)`,
		token.ID, token.UserID, token.Name, token.HashedToken, token.Scopes, nullableTime(token.ExpiresAt), token.CreatedAt, token.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("inserting api token: %w", err)
	}
	return nil
}

func (s *Store) GetAPITokenByHash(ctx context.Context, hashedToken string) (*gestalt.StoredAPIToken, error) {
	row := s.DB.QueryRowContext(ctx, `
		SELECT id, user_id, name, hashed_token, scopes, expires_at, created_at, updated_at
		FROM api_tokens WHERE hashed_token = `+s.ph(1)+`
		AND (expires_at IS NULL OR expires_at > `+s.ph(2)+`)`,
		hashedToken, time.Now(),
	)
	token, err := scanAPIToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting api token by hash: %w", err)
	}
	return token, nil
}

func (s *Store) ListAPITokens(ctx context.Context, userID string) ([]*gestalt.StoredAPIToken, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, user_id, name, hashed_token, scopes, expires_at, created_at, updated_at
		FROM api_tokens WHERE user_id = `+s.ph(1), userID)
	if err != nil {
		return nil, fmt.Errorf("listing api tokens: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*gestalt.StoredAPIToken
	for rows.Next() {
		token, err := scanAPIToken(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning api token row: %w", err)
		}
		out = append(out, token)
	}
	return out, rows.Err()
}

func (s *Store) RevokeAPIToken(ctx context.Context, userID, id string) error {
	result, err := s.DB.ExecContext(ctx, "DELETE FROM api_tokens WHERE id = "+s.ph(1)+" AND user_id = "+s.ph(2), id, userID)
	if err != nil {
		return fmt.Errorf("revoking api token: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("revoking api token: %w", err)
	}
	if affected == 0 {
		return status.Errorf(codes.NotFound, "api token %s for user %s not found", id, userID)
	}
	return nil
}

func (s *Store) RevokeAllAPITokens(ctx context.Context, userID string) (int64, error) {
	result, err := s.DB.ExecContext(ctx, "DELETE FROM api_tokens WHERE user_id = "+s.ph(1), userID)
	if err != nil {
		return 0, fmt.Errorf("revoking all api tokens: %w", err)
	}
	revoked, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("revoking all api tokens: %w", err)
	}
	return revoked, nil
}

const defaultRegistrationDDL = `CREATE TABLE IF NOT EXISTS oauth_registrations (
	id VARCHAR(36) PRIMARY KEY,
	auth_server_url VARCHAR(255) NOT NULL,
	redirect_uri VARCHAR(255) NOT NULL,
	client_id VARCHAR(255) NOT NULL,
	client_secret_encrypted TEXT,
	expires_at DATETIME NULL,
	authorization_endpoint VARCHAR(500) NOT NULL,
	token_endpoint VARCHAR(500) NOT NULL,
	scopes_supported TEXT,
	discovered_at DATETIME NOT NULL,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL,
	UNIQUE (auth_server_url, redirect_uri)
)`

func (s *Store) OAuthRegistrationDDL() string {
	ddl := defaultRegistrationDDL
	if provider, ok := s.Dialect.(RegistrationDDLProvider); ok {
		ddl = provider.RegistrationDDL()
	}
	return ddl
}

func (s *Store) MigrateOAuthRegistrations(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, s.OAuthRegistrationDDL())
	return err
}

func (s *Store) GetOAuthRegistration(ctx context.Context, authServerURL, redirectURI string) (*gestalt.OAuthRegistration, error) {
	row := s.DB.QueryRowContext(ctx, `SELECT auth_server_url, redirect_uri, client_id, client_secret_encrypted,
		expires_at, authorization_endpoint, token_endpoint, scopes_supported, discovered_at
		FROM oauth_registrations
		WHERE auth_server_url = `+s.ph(1)+` AND redirect_uri = `+s.ph(2), authServerURL, redirectURI)

	var registration gestalt.OAuthRegistration
	var secret sql.NullString
	var expiresAt sql.NullTime
	var scopes sql.NullString
	err := row.Scan(
		&registration.AuthServerURL,
		&registration.RedirectURI,
		&registration.ClientID,
		&secret,
		&expiresAt,
		&registration.AuthorizationEndpoint,
		&registration.TokenEndpoint,
		&scopes,
		&registration.DiscoveredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying oauth registration: %w", err)
	}
	registration.ClientSecretSealed, err = sealcodec.Decode(secret.String)
	if err != nil {
		return nil, fmt.Errorf("decode oauth client secret: %w", err)
	}
	if expiresAt.Valid {
		registration.ExpiresAt = &expiresAt.Time
	}
	registration.ScopesSupported = scopes.String
	return &registration, nil
}

func (s *Store) PutOAuthRegistration(ctx context.Context, registration *gestalt.OAuthRegistration) error {
	now := time.Now().UTC().Truncate(time.Second)

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning oauth registration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, `UPDATE oauth_registrations SET
		client_id = `+s.ph(1)+`,
		client_secret_encrypted = `+s.ph(2)+`,
		expires_at = `+s.ph(3)+`,
		authorization_endpoint = `+s.ph(4)+`,
		token_endpoint = `+s.ph(5)+`,
		scopes_supported = `+s.ph(6)+`,
		discovered_at = `+s.ph(7)+`,
		updated_at = `+s.ph(8)+`
		WHERE auth_server_url = `+s.ph(9)+` AND redirect_uri = `+s.ph(10),
		registration.ClientID,
		sealcodec.Encode(registration.ClientSecretSealed),
		nullableTime(registration.ExpiresAt),
		registration.AuthorizationEndpoint,
		registration.TokenEndpoint,
		registration.ScopesSupported,
		registration.DiscoveredAt,
		now,
		registration.AuthServerURL,
		registration.RedirectURI,
	)
	if err != nil {
		return fmt.Errorf("updating oauth registration: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO oauth_registrations
			(id, auth_server_url, redirect_uri, client_id, client_secret_encrypted,
			 expires_at, authorization_endpoint, token_endpoint, scopes_supported, discovered_at, created_at, updated_at)
			VALUES (`+s.ph(1)+`, `+s.ph(2)+`, `+s.ph(3)+`, `+s.ph(4)+`, `+s.ph(5)+`, `+s.ph(6)+`, `+s.ph(7)+`, `+s.ph(8)+`, `+s.ph(9)+`, `+s.ph(10)+`, `+s.ph(11)+`, `+s.ph(12)+`)`,
			uuid.NewString(),
			registration.AuthServerURL,
			registration.RedirectURI,
			registration.ClientID,
			sealcodec.Encode(registration.ClientSecretSealed),
			nullableTime(registration.ExpiresAt),
			registration.AuthorizationEndpoint,
			registration.TokenEndpoint,
			registration.ScopesSupported,
			registration.DiscoveredAt,
			now,
			now,
		)
		if err != nil {
			return fmt.Errorf("inserting oauth registration: %w", err)
		}
	}

	return tx.Commit()
}

func (s *Store) DeleteOAuthRegistration(ctx context.Context, authServerURL, redirectURI string) error {
	_, err := s.DB.ExecContext(ctx,
		`DELETE FROM oauth_registrations WHERE auth_server_url = `+s.ph(1)+` AND redirect_uri = `+s.ph(2),
		authServerURL, redirectURI,
	)
	if err != nil {
		return fmt.Errorf("deleting oauth registration: %w", err)
	}
	return nil
}
