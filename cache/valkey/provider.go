package valkey

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valon-technologies/gestalt-providers/cache/internal/configutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	providerVersion     = "0.0.1-alpha.1"
	defaultDialTimeout  = 5 * time.Second
	defaultWriteTimeout = 5 * time.Second
)

type config struct {
	Addresses    []string      `yaml:"addresses"`
	Username     string        `yaml:"username"`
	Password     string        `yaml:"password"`
	DB           int           `yaml:"db"`
	TLS          bool          `yaml:"tls"`
	DialTimeout  time.Duration `yaml:"dialTimeout"`
	WriteTimeout time.Duration `yaml:"writeTimeout"`
}

type Provider struct {
	proto.UnimplementedCacheServer

	name   string
	client valkey.Client
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("valkey cache: %w", err)
	}
	if len(cfg.Addresses) == 0 {
		return fmt.Errorf("valkey cache: addresses is required")
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}

	opts := valkey.ClientOption{
		InitAddress:       append([]string(nil), cfg.Addresses...),
		ForceSingleClient: true,
		Username:          cfg.Username,
		Password:          cfg.Password,
		SelectDB:          cfg.DB,
		ConnWriteTimeout:  cfg.WriteTimeout,
		Dialer: net.Dialer{
			Timeout: cfg.DialTimeout,
		},
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	client, err := valkey.NewClient(opts)
	if err != nil {
		return fmt.Errorf("valkey cache: create client: %w", err)
	}
	if err := ping(ctx, client); err != nil {
		client.Close()
		return fmt.Errorf("valkey cache: ping: %w", err)
	}

	if p.client != nil {
		p.client.Close()
	}
	p.name = name
	p.client = client
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	name := p.name
	if name == "" {
		name = "valkey"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindCache,
		Name:        name,
		DisplayName: "Valkey",
		Description: "Valkey-backed cache provider.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	client, err := p.configured()
	if err != nil {
		return err
	}
	return ping(ctx, client)
}

func (p *Provider) Get(ctx context.Context, req *proto.CacheGetRequest) (*proto.CacheGetResponse, error) {
	client, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	value, found, err := get(ctx, client, req.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "valkey cache: get %q: %v", req.GetKey(), err)
	}
	return &proto.CacheGetResponse{Found: found, Value: value}, nil
}

func (p *Provider) GetMany(ctx context.Context, req *proto.CacheGetManyRequest) (*proto.CacheGetManyResponse, error) {
	client, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	values, err := getMany(ctx, client, req.GetKeys())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "valkey cache: get many: %v", err)
	}

	entries := make([]*proto.CacheResult, 0, len(req.GetKeys()))
	for _, key := range req.GetKeys() {
		entry := &proto.CacheResult{Key: key}
		if value, ok := values[key]; ok {
			entry.Found = true
			entry.Value = value
		}
		entries = append(entries, entry)
	}
	return &proto.CacheGetManyResponse{Entries: entries}, nil
}

func (p *Provider) Set(ctx context.Context, req *proto.CacheSetRequest) (*emptypb.Empty, error) {
	client, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	ttl, err := ttlFromProto(req.GetTtl())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := set(ctx, client, req.GetKey(), req.GetValue(), ttl); err != nil {
		return nil, status.Errorf(codes.Internal, "valkey cache: set %q: %v", req.GetKey(), err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) SetMany(ctx context.Context, req *proto.CacheSetManyRequest) (*emptypb.Empty, error) {
	client, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	ttl, err := ttlFromProto(req.GetTtl())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := setMany(ctx, client, req.GetEntries(), ttl); err != nil {
		return nil, status.Errorf(codes.Internal, "valkey cache: set many: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) Delete(ctx context.Context, req *proto.CacheDeleteRequest) (*proto.CacheDeleteResponse, error) {
	client, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	deleted, err := del(ctx, client, req.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "valkey cache: delete %q: %v", req.GetKey(), err)
	}
	return &proto.CacheDeleteResponse{Deleted: deleted}, nil
}

func (p *Provider) DeleteMany(ctx context.Context, req *proto.CacheDeleteManyRequest) (*proto.CacheDeleteManyResponse, error) {
	client, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	deleted, err := deleteMany(ctx, client, req.GetKeys())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "valkey cache: delete many: %v", err)
	}
	return &proto.CacheDeleteManyResponse{Deleted: deleted}, nil
}

func (p *Provider) Touch(ctx context.Context, req *proto.CacheTouchRequest) (*proto.CacheTouchResponse, error) {
	client, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	ttl, err := ttlFromProto(req.GetTtl())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	touched, err := touch(ctx, client, req.GetKey(), ttl)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "valkey cache: touch %q: %v", req.GetKey(), err)
	}
	return &proto.CacheTouchResponse{Touched: touched}, nil
}

func (p *Provider) Close() error {
	if p.client != nil {
		p.client.Close()
		p.client = nil
	}
	return nil
}

func (p *Provider) configured() (valkey.Client, error) {
	if p.client == nil {
		return nil, fmt.Errorf("valkey cache: not configured")
	}
	return p.client, nil
}

func get(ctx context.Context, client valkey.Client, key string) ([]byte, bool, error) {
	value, err := client.Do(ctx, client.B().Get().Key(key).Build()).AsBytes()
	if valkey.IsValkeyNil(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), value...), true, nil
}

func getMany(ctx context.Context, client valkey.Client, keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}
	values, err := client.Do(ctx, client.B().Mget().Key(keys...).Build()).ToArray()
	if err != nil {
		return nil, err
	}
	if len(values) != len(keys) {
		return nil, fmt.Errorf("expected %d values, got %d", len(keys), len(values))
	}
	out := make(map[string][]byte, len(keys))
	for i, key := range keys {
		value, err := values[i].AsBytes()
		if valkey.IsValkeyNil(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("decode %q: %w", key, err)
		}
		out[key] = append([]byte(nil), value...)
	}
	return out, nil
}

func set(ctx context.Context, client valkey.Client, key string, value []byte, ttl time.Duration) error {
	ttl = valkeyTTL(ttl)
	cmd := client.B().Set().Key(key).Value(valkey.BinaryString(value))
	if ttl > 0 {
		return client.Do(ctx, cmd.Px(ttl).Build()).Error()
	}
	return client.Do(ctx, cmd.Build()).Error()
}

func setMany(ctx context.Context, client valkey.Client, entries []*proto.CacheSetEntry, ttl time.Duration) error {
	if len(entries) == 0 {
		return nil
	}
	ttl = valkeyTTL(ttl)

	cmds := make([]valkey.Completed, 0, len(entries))
	for _, entry := range entries {
		cmd := client.B().Set().Key(entry.GetKey()).Value(valkey.BinaryString(entry.GetValue()))
		if ttl > 0 {
			cmds = append(cmds, cmd.Px(ttl).Build())
			continue
		}
		cmds = append(cmds, cmd.Build())
	}

	results := client.DoMulti(ctx, cmds...)
	for i, result := range results {
		if err := result.Error(); err != nil {
			return fmt.Errorf("set %q: %w", entries[i].GetKey(), err)
		}
	}
	return nil
}

func del(ctx context.Context, client valkey.Client, key string) (bool, error) {
	deleted, err := client.Do(ctx, client.B().Del().Key(key).Build()).ToInt64()
	if err != nil {
		return false, err
	}
	return deleted > 0, nil
}

func deleteMany(ctx context.Context, client valkey.Client, keys []string) (int64, error) {
	unique := uniqueKeys(keys)
	if len(unique) == 0 {
		return 0, nil
	}
	return client.Do(ctx, client.B().Del().Key(unique...).Build()).ToInt64()
}

func touch(ctx context.Context, client valkey.Client, key string, ttl time.Duration) (bool, error) {
	if ttl == 0 {
		exists, err := client.Do(ctx, client.B().Exists().Key(key).Build()).ToInt64()
		if err != nil {
			return false, err
		}
		if exists == 0 {
			return false, nil
		}
		if err := client.Do(ctx, client.B().Persist().Key(key).Build()).Error(); err != nil {
			return false, err
		}
		return true, nil
	}
	ttl = valkeyTTL(ttl)

	touched, err := client.Do(ctx, client.B().Pexpire().Key(key).Milliseconds(ttl.Milliseconds()).Build()).ToInt64()
	if err != nil {
		return false, err
	}
	return touched > 0, nil
}

func ping(ctx context.Context, client valkey.Client) error {
	return client.Do(ctx, client.B().Ping().Build()).Error()
}

func validatedTTL(ttl time.Duration) (time.Duration, error) {
	if ttl < 0 {
		return 0, fmt.Errorf("ttl must be >= 0")
	}
	return ttl, nil
}

func valkeyTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	if ttl%time.Millisecond == 0 {
		return ttl
	}
	return ((ttl / time.Millisecond) + 1) * time.Millisecond
}

func ttlFromProto(ttl *durationpb.Duration) (time.Duration, error) {
	if ttl == nil {
		return 0, nil
	}
	return validatedTTL(ttl.AsDuration())
}

func uniqueKeys(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

var _ gestalt.CacheProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
