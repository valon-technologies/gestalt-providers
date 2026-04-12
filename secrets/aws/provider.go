package aws

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
)

const (
	providerVersion     = "0.0.1-alpha.1"
	defaultVersionStage = "AWSCURRENT"
	defaultTimeout      = 10 * time.Second
)

type config struct {
	Region       string `yaml:"region"`
	VersionStage string `yaml:"versionStage"`
	Endpoint     string `yaml:"endpoint"`
}

type Provider struct {
	name         string
	client       *secretsmanager.Client
	versionStage string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("aws secrets: %w", err)
	}
	if cfg.Region == "" {
		return fmt.Errorf("aws secrets: region is required")
	}
	if cfg.VersionStage == "" {
		cfg.VersionStage = defaultVersionStage
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return fmt.Errorf("aws secrets: loading config: %w", err)
	}

	var clientOpts []func(*secretsmanager.Options)
	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *secretsmanager.Options) {
			o.BaseEndpoint = &cfg.Endpoint
		})
	}

	p.name = name
	p.client = secretsmanager.NewFromConfig(awsCfg, clientOpts...)
	p.versionStage = cfg.VersionStage
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "AWS Secrets Manager",
		Description: "Resolves secrets from AWS Secrets Manager.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	input := &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(name),
		VersionStage: aws.String(p.versionStage),
	}

	resp, err := p.client.GetSecretValue(ctx, input)
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, name)
		}
		return "", fmt.Errorf("accessing secret %q: %w", name, err)
	}
	if resp.SecretString == nil {
		return "", fmt.Errorf("secret %q has no string value (binary secrets are not supported)", name)
	}
	return *resp.SecretString, nil
}
