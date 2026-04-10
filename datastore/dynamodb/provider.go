package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"gopkg.in/yaml.v3"
)

const providerVersion = "0.0.1-alpha.1"

type config struct {
	Table    string `yaml:"table"`
	Region   string `yaml:"region"`
	Endpoint string `yaml:"endpoint"`
}

// Provider implements gestalt.IndexedDBProvider.
type Provider struct {
	proto.UnimplementedIndexedDBServer
	store *store
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(ctx context.Context, _ string, raw map[string]any) error {
	var cfg config
	b, err := yaml.Marshal(raw)
	if err != nil {
		return fmt.Errorf("dynamodb: marshal config: %w", err)
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("dynamodb: decode config: %w", err)
	}
	if cfg.Table == "" {
		cfg.Table = "gestalt"
	}

	client, err := newClient(cfg)
	if err != nil {
		return fmt.Errorf("dynamodb: create client: %w", err)
	}

	s := &store{
		client: client,
		table:  cfg.Table,
	}

	if err := s.ensureTable(ctx); err != nil {
		return fmt.Errorf("dynamodb: ensure table: %w", err)
	}
	if err := s.loadSchemas(ctx); err != nil {
		return fmt.Errorf("dynamodb: load schemas: %w", err)
	}

	p.store = s
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindDatastore,
		Name:        "dynamodb",
		DisplayName: "DynamoDB",
		Description: "Amazon DynamoDB datastore provider.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	if p.store == nil {
		return fmt.Errorf("dynamodb: not configured")
	}
	return p.store.healthCheck(ctx)
}

func (p *Provider) Close() error { return nil }

func newClient(cfg config) (*dynamodb.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.Endpoint != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("local", "local", ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	var clientOpts []func(*dynamodb.Options)
	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	return dynamodb.NewFromConfig(awsCfg, clientOpts...), nil
}
