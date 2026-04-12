package vault

import (
	"context"
	"fmt"
	"strings"
	"time"

	vaultapi "github.com/hashicorp/vault/api"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
)

const (
	providerVersion  = "0.0.1-alpha.1"
	defaultTimeout   = 10 * time.Second
	defaultMountPath = "secret"
	kvDataKey        = "value"
)

type config struct {
	Address   string `yaml:"address"`
	Token     string `yaml:"token"`
	MountPath string `yaml:"mountPath"`
	Namespace string `yaml:"namespace"`
}

type Provider struct {
	name      string
	client    *vaultapi.Client
	mountPath string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("vault secrets: %w", err)
	}
	if cfg.Address == "" {
		return fmt.Errorf("vault secrets: address is required")
	}
	if cfg.Token == "" {
		return fmt.Errorf("vault secrets: token is required")
	}
	if cfg.MountPath == "" {
		cfg.MountPath = defaultMountPath
	}

	vaultCfg := vaultapi.DefaultConfig()
	vaultCfg.Address = cfg.Address

	client, err := vaultapi.NewClient(vaultCfg)
	if err != nil {
		return fmt.Errorf("vault secrets: creating client: %w", err)
	}
	client.SetToken(cfg.Token)

	if cfg.Namespace != "" {
		client.SetNamespace(cfg.Namespace)
	}

	p.name = name
	p.client = client
	p.mountPath = cfg.MountPath
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "HashiCorp Vault",
		Description: "Resolves secrets from HashiCorp Vault KV v2.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(ctx context.Context, name string) (string, error) {
	if strings.Contains(name, "/") {
		return "", fmt.Errorf("invalid secret name %q: must not contain '/'", name)
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	path := fmt.Sprintf("%s/data/%s", p.mountPath, name)
	secret, err := p.client.Logical().ReadWithContext(ctx, path)
	if err != nil {
		return "", fmt.Errorf("accessing secret %q: %w", name, err)
	}
	if secret == nil || secret.Data == nil {
		return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, name)
	}

	data, ok := secret.Data["data"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("%w: %q (no data in KV v2 response)", gestalt.ErrSecretNotFound, name)
	}

	value, ok := data[kvDataKey].(string)
	if !ok {
		return "", fmt.Errorf("secret %q: missing or non-string %q key in KV data", name, kvDataKey)
	}
	return value, nil
}
