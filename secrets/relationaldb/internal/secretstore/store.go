package secretstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	_ "github.com/go-sql-driver/mysql"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const tableName = "_gestalt_secrets"
const MaxPlaintextBytes = 64 * 1024

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Config struct {
	DSN    string
	Schema string
	KMSKey string
}

type Store struct {
	db    *sql.DB
	kms   *kms.KeyManagementClient
	table string
	key   string
}

func Open(ctx context.Context, cfg Config) (*Store, error) {
	cfg.DSN = strings.TrimSpace(cfg.DSN)
	cfg.Schema = strings.TrimSpace(cfg.Schema)
	cfg.KMSKey = strings.TrimSpace(cfg.KMSKey)
	if cfg.DSN == "" {
		return nil, fmt.Errorf("dsn is required")
	}
	if !identifierPattern.MatchString(cfg.Schema) {
		return nil, fmt.Errorf("schema must be a non-empty SQL identifier")
	}
	if cfg.KMSKey == "" {
		return nil, fmt.Errorf("kmsKey is required")
	}

	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect database: %w", err)
	}

	kmsClient, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create KMS client: %w", err)
	}

	return &Store{
		db:    db,
		kms:   kmsClient,
		table: "`" + cfg.Schema + "`.`" + tableName + "`",
		key:   cfg.KMSKey,
	}, nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	if s.db != nil {
		errs = append(errs, s.db.Close())
	}
	if s.kms != nil {
		errs = append(errs, s.kms.Close())
	}
	return errors.Join(errs...)
}

func (s *Store) Get(ctx context.Context, name string) (string, error) {
	name, err := validateName(name)
	if err != nil {
		return "", err
	}

	var ciphertext []byte
	err = s.db.QueryRowContext(ctx, "SELECT ciphertext FROM "+s.table+" WHERE name = ?", name).Scan(&ciphertext)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, name)
	}
	if err != nil {
		return "", fmt.Errorf("read secret %q: %w", name, err)
	}

	response, err := s.kms.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:                        s.key,
		Ciphertext:                  ciphertext,
		AdditionalAuthenticatedData: []byte(name),
	})
	if err != nil {
		return "", fmt.Errorf("decrypt secret %q: %w", name, err)
	}
	return string(response.Plaintext), nil
}

func (s *Store) Initialize(ctx context.Context) error {
	query := "CREATE TABLE IF NOT EXISTS " + s.table + " (" +
		"name VARBINARY(255) NOT NULL PRIMARY KEY, " +
		"ciphertext LONGBLOB NOT NULL, " +
		"updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)" +
		")"
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("initialize secrets table: %w", err)
	}
	return nil
}

func (s *Store) Put(ctx context.Context, name string, plaintext []byte) error {
	name, err := validateName(name)
	if err != nil {
		return err
	}
	if len(plaintext) == 0 {
		return fmt.Errorf("secret value must not be empty")
	}
	if len(plaintext) > MaxPlaintextBytes {
		return fmt.Errorf("secret value must not exceed %d bytes", MaxPlaintextBytes)
	}

	response, err := s.kms.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:                        s.key,
		Plaintext:                   plaintext,
		AdditionalAuthenticatedData: []byte(name),
	})
	if err != nil {
		return fmt.Errorf("encrypt secret %q: %w", name, err)
	}

	query := "INSERT INTO " + s.table + " (name, ciphertext) VALUES (?, ?) " +
		"ON DUPLICATE KEY UPDATE ciphertext = VALUES(ciphertext), updated_at = CURRENT_TIMESTAMP(6)"
	if _, err := s.db.ExecContext(ctx, query, name, response.Ciphertext); err != nil {
		return fmt.Errorf("store secret %q: %w", name, err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, name string) error {
	name, err := validateName(name)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM "+s.table+" WHERE name = ?", name); err != nil {
		return fmt.Errorf("delete secret %q: %w", name, err)
	}
	return nil
}

func (s *Store) List(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT name FROM "+s.table+" ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("list secrets: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	return names, nil
}

func validateName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("secret name is required")
	}
	if len(name) > 255 {
		return "", fmt.Errorf("secret name must be at most 255 bytes")
	}
	return name, nil
}
