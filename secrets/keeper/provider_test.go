package keeper

import (
	"context"
	"errors"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type stubNotationClient struct {
	getNotationResults func(notation string) ([]string, error)
}

func (s stubNotationClient) GetNotationResults(notation string) ([]string, error) {
	return s.getNotationResults(notation)
}

func TestGetSecretByUIDUsesDefaultField(t *testing.T) {
	t.Parallel()

	var gotNotation string
	provider := Provider{
		client: stubNotationClient{
			getNotationResults: func(notation string) ([]string, error) {
				gotNotation = notation
				return []string{"secret-value"}, nil
			},
		},
		field: "password",
	}

	value, err := provider.GetSecret(context.Background(), "record-uid")
	if err != nil {
		t.Fatalf("GetSecret() error = %v", err)
	}
	if value != "secret-value" {
		t.Fatalf("GetSecret() value = %q, want %q", value, "secret-value")
	}
	if gotNotation != "keeper://record-uid/field/password" {
		t.Fatalf("GetSecret() notation = %q, want %q", gotNotation, "keeper://record-uid/field/password")
	}
}

func TestGetSecretReturnsNotFoundForMissingNotation(t *testing.T) {
	t.Parallel()

	provider := Provider{
		client: stubNotationClient{
			getNotationResults: func(notation string) ([]string, error) {
				return nil, errors.New("notation keeper://record/field/password has no fields matching")
			},
		},
		field: "password",
	}

	_, err := provider.GetSecret(context.Background(), "keeper://record/field/password")
	if !errors.Is(err, gestalt.ErrSecretNotFound) {
		t.Fatalf("GetSecret() error = %v, want ErrSecretNotFound", err)
	}
}

func TestGetSecretHonorsContextTimeout(t *testing.T) {
	t.Parallel()

	blocked := make(chan struct{})
	provider := Provider{
		client: stubNotationClient{
			getNotationResults: func(notation string) ([]string, error) {
				<-blocked
				return []string{"secret-value"}, nil
			},
		},
		field: "password",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := provider.GetSecret(ctx, "keeper://record/field/password")
	close(blocked)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GetSecret() error = %v, want context deadline exceeded", err)
	}
}
