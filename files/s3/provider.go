package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion = "0.0.1-alpha.1"
	streamChunkSize = 64 * 1024

	metaKind         = "gestalt-kind"
	metaName         = "gestalt-name"
	metaLastModified = "gestalt-last-modified"

	objectURLScheme = "blob:gestalt-s3/"
)

var errNotConfigured = errors.New("s3 fileapi: not configured")

type config struct {
	Bucket         string `yaml:"bucket"`
	Region         string `yaml:"region"`
	Prefix         string `yaml:"prefix"`
	Endpoint       string `yaml:"endpoint"`
	ForcePathStyle bool   `yaml:"forcePathStyle"`
}

type s3Client interface {
	HeadBucket(context.Context, *s3sdk.HeadBucketInput, ...func(*s3sdk.Options)) (*s3sdk.HeadBucketOutput, error)
	PutObject(context.Context, *s3sdk.PutObjectInput, ...func(*s3sdk.Options)) (*s3sdk.PutObjectOutput, error)
	HeadObject(context.Context, *s3sdk.HeadObjectInput, ...func(*s3sdk.Options)) (*s3sdk.HeadObjectOutput, error)
	GetObject(context.Context, *s3sdk.GetObjectInput, ...func(*s3sdk.Options)) (*s3sdk.GetObjectOutput, error)
}

type objectURLStore struct {
	mu   sync.RWMutex
	urls map[string]string
}

type Provider struct {
	proto.UnimplementedFileAPIServer

	mu         sync.RWMutex
	name       string
	cfg        config
	client     s3Client
	newClient  func(context.Context, config) (s3Client, error)
	objectURLs objectURLStore
}

func New() *Provider {
	return &Provider{newClient: buildS3Client}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	var cfg config
	data, err := yaml.Marshal(raw)
	if err != nil {
		return fmt.Errorf("s3 fileapi: marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("s3 fileapi: decode config: %w", err)
	}
	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	cfg.Region = strings.TrimSpace(cfg.Region)
	cfg.Prefix = normalizePrefix(cfg.Prefix)
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)

	switch {
	case cfg.Bucket == "":
		return fmt.Errorf("s3 fileapi: bucket is required")
	case cfg.Region == "":
		return fmt.Errorf("s3 fileapi: region is required")
	}

	clientBuilder := p.newClient
	if clientBuilder == nil {
		clientBuilder = buildS3Client
	}
	client, err := clientBuilder(ctx, cfg)
	if err != nil {
		return fmt.Errorf("s3 fileapi: create client: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.name = name
	p.cfg = cfg
	p.client = client
	p.objectURLs = objectURLStore{urls: make(map[string]string)}
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	name := p.name
	if strings.TrimSpace(name) == "" {
		name = "s3"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindFileAPI,
		Name:        name,
		DisplayName: "Amazon S3",
		Description: "Amazon S3-backed FileAPI provider.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	client, cfg, err := p.configured()
	if err != nil {
		return err
	}
	_, err = client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: aws.String(cfg.Bucket)})
	if err != nil {
		return fmt.Errorf("s3 fileapi: head bucket: %w", err)
	}
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client = nil
	p.objectURLs = objectURLStore{}
	return nil
}

func (p *Provider) CreateBlob(ctx context.Context, req *proto.CreateBlobRequest) (*proto.FileObjectResponse, error) {
	options := req.GetOptions()
	var mimeType string
	var endings proto.LineEndings
	if options != nil {
		mimeType = options.GetMimeType()
		endings = options.GetEndings()
	}
	info, err := p.createObject(ctx, objectKindBlob, req.GetParts(), "", 0, mimeType, endings)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &proto.FileObjectResponse{Object: info}, nil
}

func (p *Provider) CreateFile(ctx context.Context, req *proto.CreateFileRequest) (*proto.FileObjectResponse, error) {
	options := req.GetOptions()
	var mimeType string
	var endings proto.LineEndings
	var lastModified int64
	if options != nil {
		mimeType = options.GetMimeType()
		endings = options.GetEndings()
		lastModified = resolveLastModified(options.GetLastModified())
	}
	info, err := p.createObject(
		ctx,
		objectKindFile,
		req.GetFileBits(),
		req.GetFileName(),
		lastModified,
		mimeType,
		endings,
	)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &proto.FileObjectResponse{Object: info}, nil
}

func (p *Provider) Stat(ctx context.Context, req *proto.FileObjectRequest) (*proto.FileObjectResponse, error) {
	info, err := p.statObject(ctx, req.GetId())
	if err != nil {
		return nil, toStatusError(err)
	}
	return &proto.FileObjectResponse{Object: info}, nil
}

func (p *Provider) Slice(ctx context.Context, req *proto.SliceRequest) (*proto.FileObjectResponse, error) {
	if _, err := p.statObject(ctx, req.GetId()); err != nil {
		return nil, toStatusError(err)
	}

	data, err := p.readObjectBytes(ctx, req.GetId())
	if err != nil {
		return nil, toStatusError(err)
	}
	sliced := sliceBytes(data, req.Start, req.End)
	blob, err := p.writeObject(ctx, objectSpec{
		Kind:         objectKindBlob,
		Bytes:        sliced,
		ContentType:  normalizeType(req.GetContentType()),
		LastModified: 0,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	return &proto.FileObjectResponse{Object: blob}, nil
}

func (p *Provider) ReadBytes(ctx context.Context, req *proto.FileObjectRequest) (*proto.BytesResponse, error) {
	data, err := p.readObjectBytes(ctx, req.GetId())
	if err != nil {
		return nil, toStatusError(err)
	}
	return &proto.BytesResponse{Data: data}, nil
}

func (p *Provider) OpenReadStream(req *proto.ReadStreamRequest, stream proto.FileAPI_OpenReadStreamServer) error {
	reader, err := p.openObjectReader(stream.Context(), req.GetId())
	if err != nil {
		return toStatusError(err)
	}
	defer reader.Close()

	buf := make([]byte, streamChunkSize)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			if err := stream.Send(&proto.ReadChunk{Data: chunk}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return toStatusError(fmt.Errorf("s3 fileapi: read stream: %w", readErr))
		}
	}
}

func (p *Provider) CreateObjectURL(ctx context.Context, req *proto.CreateObjectURLRequest) (*proto.ObjectURLResponse, error) {
	if _, err := p.statObject(ctx, req.GetId()); err != nil {
		return nil, toStatusError(err)
	}
	url := objectURLScheme + uuid.NewString()
	p.objectURLs.mu.Lock()
	if p.objectURLs.urls == nil {
		p.objectURLs.urls = make(map[string]string)
	}
	p.objectURLs.urls[url] = req.GetId()
	p.objectURLs.mu.Unlock()
	return &proto.ObjectURLResponse{Url: url}, nil
}

func (p *Provider) ResolveObjectURL(ctx context.Context, req *proto.ObjectURLRequest) (*proto.FileObjectResponse, error) {
	p.objectURLs.mu.RLock()
	id, ok := p.objectURLs.urls[req.GetUrl()]
	p.objectURLs.mu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "object url not found")
	}
	info, err := p.statObject(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &proto.FileObjectResponse{Object: info}, nil
}

func (p *Provider) RevokeObjectURL(_ context.Context, req *proto.ObjectURLRequest) (*emptypb.Empty, error) {
	p.objectURLs.mu.Lock()
	delete(p.objectURLs.urls, req.GetUrl())
	p.objectURLs.mu.Unlock()
	return &emptypb.Empty{}, nil
}

type objectKind string

const (
	objectKindBlob objectKind = "blob"
	objectKindFile objectKind = "file"
)

type objectSpec struct {
	Kind         objectKind
	Bytes        []byte
	ContentType  string
	Name         string
	LastModified int64
}

func (p *Provider) createObject(
	ctx context.Context,
	kind objectKind,
	parts []*proto.BlobPart,
	name string,
	lastModified int64,
	contentType string,
	endings proto.LineEndings,
) (*proto.FileObject, error) {
	data, err := p.resolveParts(ctx, parts, endings)
	if err != nil {
		return nil, err
	}
	return p.writeObject(ctx, objectSpec{
		Kind:         kind,
		Bytes:        data,
		ContentType:  normalizeType(contentType),
		Name:         name,
		LastModified: lastModified,
	})
}

func (p *Provider) resolveParts(ctx context.Context, parts []*proto.BlobPart, endings proto.LineEndings) ([]byte, error) {
	var out []byte
	for _, part := range parts {
		switch value := part.GetKind().(type) {
		case *proto.BlobPart_StringData:
			out = append(out, convertStringPart(value.StringData, endings)...)
		case *proto.BlobPart_BytesData:
			out = append(out, value.BytesData...)
		case *proto.BlobPart_BlobId:
			data, err := p.readObjectBytes(ctx, value.BlobId)
			if err != nil {
				return nil, err
			}
			out = append(out, data...)
		default:
			return nil, status.Error(codes.InvalidArgument, "unsupported blob part")
		}
	}
	return out, nil
}

func (p *Provider) writeObject(ctx context.Context, spec objectSpec) (*proto.FileObject, error) {
	client, cfg, err := p.configured()
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()
	key := objectKey(cfg.Prefix, id)
	metadata := map[string]string{
		metaKind: string(spec.Kind),
	}
	if spec.Name != "" {
		metadata[metaName] = spec.Name
	}
	if spec.Kind == objectKindFile {
		metadata[metaLastModified] = strconv.FormatInt(spec.LastModified, 10)
	}

	input := &s3sdk.PutObjectInput{
		Bucket:      aws.String(cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(spec.Bytes),
		ContentType: aws.String(spec.ContentType),
		Metadata:    metadata,
	}
	if _, err := client.PutObject(ctx, input); err != nil {
		return nil, fmt.Errorf("s3 fileapi: put object: %w", err)
	}

	return &proto.FileObject{
		Id:           id,
		Kind:         protoKind(spec.Kind),
		Size:         int64(len(spec.Bytes)),
		Type:         spec.ContentType,
		Name:         spec.Name,
		LastModified: spec.LastModified,
	}, nil
}

func (p *Provider) statObject(ctx context.Context, id string) (*proto.FileObject, error) {
	client, cfg, err := p.configured()
	if err != nil {
		return nil, err
	}

	out, err := client.HeadObject(ctx, &s3sdk.HeadObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(objectKey(cfg.Prefix, id)),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 fileapi: head object %q: %w", id, err)
	}

	kind := objectKind(metadataValue(out.Metadata, metaKind))
	if kind == "" {
		kind = objectKindBlob
	}
	lastModified, _ := strconv.ParseInt(metadataValue(out.Metadata, metaLastModified), 10, 64)

	return &proto.FileObject{
		Id:           id,
		Kind:         protoKind(kind),
		Size:         aws.ToInt64(out.ContentLength),
		Type:         normalizeType(aws.ToString(out.ContentType)),
		Name:         metadataValue(out.Metadata, metaName),
		LastModified: lastModified,
	}, nil
}

func (p *Provider) readObjectBytes(ctx context.Context, id string) ([]byte, error) {
	reader, err := p.openObjectReader(ctx, id)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("s3 fileapi: read object %q: %w", id, err)
	}
	return data, nil
}

func (p *Provider) openObjectReader(ctx context.Context, id string) (io.ReadCloser, error) {
	client, cfg, err := p.configured()
	if err != nil {
		return nil, err
	}

	out, err := client.GetObject(ctx, &s3sdk.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(objectKey(cfg.Prefix, id)),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 fileapi: get object %q: %w", id, err)
	}
	return out.Body, nil
}

func (p *Provider) configured() (s3Client, config, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.client == nil {
		return nil, config{}, errNotConfigured
	}
	return p.client, p.cfg, nil
}

func buildS3Client(ctx context.Context, cfg config) (s3Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}

	var opts []func(*s3sdk.Options)
	if cfg.Endpoint != "" {
		opts = append(opts, func(o *s3sdk.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}
	if cfg.ForcePathStyle {
		opts = append(opts, func(o *s3sdk.Options) {
			o.UsePathStyle = true
		})
	}
	return s3sdk.NewFromConfig(awsCfg, opts...), nil
}

func protoKind(kind objectKind) proto.FileObjectKind {
	switch kind {
	case objectKindFile:
		return proto.FileObjectKind_FILE_OBJECT_KIND_FILE
	default:
		return proto.FileObjectKind_FILE_OBJECT_KIND_BLOB
	}
}

func normalizePrefix(prefix string) string {
	return strings.Trim(strings.TrimSpace(prefix), "/")
}

func objectKey(prefix, id string) string {
	id = strings.TrimSpace(id)
	if prefix == "" {
		return "objects/" + id
	}
	return prefix + "/objects/" + id
}

func normalizeType(value string) string {
	if value == "" {
		return ""
	}
	for _, r := range value {
		if r < 0x20 || r > 0x7E {
			return ""
		}
	}
	return strings.ToLower(value)
}

func convertStringPart(value string, endings proto.LineEndings) []byte {
	if endings != proto.LineEndings_LINE_ENDINGS_NATIVE {
		return []byte(value)
	}
	normalized := strings.ReplaceAll(value, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	if runtime.GOOS == "windows" {
		normalized = strings.ReplaceAll(normalized, "\n", "\r\n")
	}
	return []byte(normalized)
}

func resolveLastModified(lastModified int64) int64 {
	if lastModified >= 0 {
		return lastModified
	}
	return time.Now().UnixMilli()
}

func sliceBounds(size int64, start, end *int64) (int64, int64) {
	relativeStart := int64(0)
	if start != nil {
		if *start < 0 {
			relativeStart = max(size+*start, 0)
		} else if *start < size {
			relativeStart = *start
		} else {
			relativeStart = size
		}
	}

	relativeEnd := size
	if end != nil {
		if *end < 0 {
			relativeEnd = max(size+*end, 0)
		} else if *end < size {
			relativeEnd = *end
		} else {
			relativeEnd = size
		}
	}
	if relativeEnd < relativeStart {
		relativeEnd = relativeStart
	}
	return relativeStart, relativeEnd
}

func sliceBytes(data []byte, start, end *int64) []byte {
	s, e := sliceBounds(int64(len(data)), start, end)
	return append([]byte(nil), data[s:e]...)
}

func metadataValue(metadata map[string]string, key string) string {
	if metadata == nil {
		return ""
	}
	if value, ok := metadata[key]; ok {
		return value
	}
	return metadata[strings.ToLower(key)]
}

func toStatusError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, errNotConfigured):
		return status.Error(codes.FailedPrecondition, err.Error())
	case isNotFound(err):
		return status.Error(codes.NotFound, err.Error())
	case isPermissionDenied(err):
		return status.Error(codes.PermissionDenied, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func isNotFound(err error) bool {
	if errors.Is(err, io.EOF) {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket":
			return true
		}
	}
	return false
}

func isPermissionDenied(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return true
		}
	}
	return false
}

var _ gestalt.FileAPIProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
