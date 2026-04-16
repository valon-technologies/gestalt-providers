package onepassword

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/1Password/connect-sdk-go/connect"
	op "github.com/1Password/connect-sdk-go/onepassword"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultField    = "password"
	defaultTimeout  = 10 * time.Second
)

type config struct {
	Host              string `yaml:"host"`
	Token             string `yaml:"token"`
	Vault             string `yaml:"vault"`
	Field             string `yaml:"field"`
	AllowInsecureHTTP bool   `yaml:"allowInsecureHttp"`
}

type itemClient interface {
	GetItem(itemQuery, vaultQuery string) (*op.Item, error)
}

type Provider struct {
	name   string
	client itemClient
	vault  string
	field  string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("onepassword secrets: %w", err)
	}
	if cfg.Host == "" {
		return fmt.Errorf("onepassword secrets: host is required")
	}
	if cfg.Token == "" {
		return fmt.Errorf("onepassword secrets: token is required")
	}
	if err := validateHostURL(cfg.Host, cfg.AllowInsecureHTTP); err != nil {
		return err
	}
	if cfg.Vault == "" {
		return fmt.Errorf("onepassword secrets: vault is required")
	}
	if cfg.Field == "" {
		cfg.Field = defaultField
	}

	p.name = name
	p.client = connect.NewClientWithUserAgent(cfg.Host, cfg.Token, "gestalt/"+providerVersion)
	p.vault = cfg.Vault
	p.field = cfg.Field
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "1Password",
		Description: "Resolves secrets from 1Password Connect.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(ctx context.Context, name string) (string, error) {
	rawName := name
	field := p.field
	if i := strings.LastIndex(name, "/"); i >= 0 {
		field = name[i+1:]
		name = name[:i]
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	item, err := p.getItem(ctx, name)
	if err != nil && name != rawName && errors.Is(err, gestalt.ErrSecretNotFound) {
		item, err = p.getItem(ctx, rawName)
		field = p.field
	}
	if err != nil {
		return "", err
	}

	value, found := fieldValue(item, field)
	if !found {
		return "", fmt.Errorf("%w: %q (field %q)", gestalt.ErrSecretNotFound, name, field)
	}
	return value, nil
}

func (p *Provider) getItem(ctx context.Context, name string) (*op.Item, error) {
	type result struct {
		item *op.Item
		err  error
	}

	resultCh := make(chan result, 1)
	go func() {
		item, err := p.client.GetItem(name, p.vault)
		resultCh <- result{item: item, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("accessing secret %q: %w", name, ctx.Err())
	case res := <-resultCh:
		if res.err != nil {
			var opErr *op.Error
			if errors.As(res.err, &opErr) && opErr.StatusCode == http.StatusNotFound {
				return nil, fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, name)
			}
			if strings.Contains(res.err.Error(), "Found 0 item(s)") {
				return nil, fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, name)
			}
			return nil, fmt.Errorf("accessing secret %q: %w", name, res.err)
		}
		return res.item, nil
	}
}

func validateHostURL(rawURL string, allowInsecureHTTP bool) error {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("onepassword secrets: host must be a valid URL: %w", err)
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return fmt.Errorf("onepassword secrets: host must be an absolute URL")
	}

	switch strings.ToLower(parsedURL.Scheme) {
	case "https":
		return nil
	case "http":
		if !allowInsecureHTTP {
			return fmt.Errorf("onepassword secrets: host must use https unless allowInsecureHttp is true for local loopback development")
		}
		if !isLoopbackHost(parsedURL.Hostname()) {
			return fmt.Errorf("onepassword secrets: host may use http only for localhost or loopback IPs when allowInsecureHttp is true")
		}
		return nil
	default:
		return fmt.Errorf("onepassword secrets: host must use https")
	}
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}

	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func fieldValue(item *op.Item, field string) (string, bool) {
	if item == nil || len(item.Fields) == 0 {
		return "", false
	}

	sectionFilter := false
	sectionLabel := ""
	fieldLabel := field
	if strings.Contains(field, ".") {
		parts := strings.Split(field, ".")
		if len(parts) == 2 {
			sectionFilter = true
			sectionLabel = parts[0]
			fieldLabel = parts[1]
		}
	}

	for _, f := range item.Fields {
		if sectionFilter && f.Section != nil && sectionLabel != item.SectionLabelForID(f.Section.ID) {
			continue
		}
		if fieldLabel == f.Label {
			return f.Value, true
		}
	}

	return "", false
}
