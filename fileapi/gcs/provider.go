package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultTimeout  = 10 * time.Second
	streamChunkSize = 64 * 1024

	objectURLPrefix = "gestalt+gcs://object/"

	metaKind         = "gestalt_fileapi_kind"
	metaName         = "gestalt_fileapi_name"
	metaLastModified = "gestalt_fileapi_last_modified"
)

type config struct {
	Bucket string `yaml:"bucket"`
	Prefix string `yaml:"prefix"`
}

type Provider struct {
	proto.UnimplementedFileAPIServer

	mu      sync.RWMutex
	name    string
	bucket  string
	prefix  string
	store   objectStore
	urls    *objectURLStore
	newID   func() string
	now     func() time.Time
	timeout time.Duration
}

type objectAttrs struct {
	ID          string
	Size        int64
	ContentType string
	Metadata    map[string]string
	Updated     time.Time
}

type objectStore interface {
	HealthCheck(context.Context) error
	Stat(context.Context, string) (*objectAttrs, error)
	Write(context.Context, string, []byte, string, map[string]string) (*objectAttrs, error)
	ReadAll(context.Context, string) ([]byte, error)
	OpenRange(context.Context, string, int64, int64) (io.ReadCloser, error)
	Close() error
}

type gcsStore struct {
	client     *storage.Client
	bucket     *storage.BucketHandle
	bucketName string
}

type objectURLStore struct {
	mu       sync.RWMutex
	values   map[string]string
	newToken func() string
}

func New() *Provider {
	return &Provider{
		urls:    newObjectURLStore(uuid.NewString),
		newID:   uuid.NewString,
		now:     time.Now,
		timeout: defaultTimeout,
	}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	p.initDefaults()

	var cfg config
	data, err := yaml.Marshal(raw)
	if err != nil {
		return fmt.Errorf("gcs fileapi: marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("gcs fileapi: decode config: %w", err)
	}
	if strings.TrimSpace(cfg.Bucket) == "" {
		return fmt.Errorf("gcs fileapi: bucket is required")
	}

	configureCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	store, err := newGCSStore(configureCtx, strings.TrimSpace(cfg.Bucket))
	if err != nil {
		return fmt.Errorf("gcs fileapi: creating storage client: %w", err)
	}

	p.mu.Lock()
	oldStore := p.store
	p.name = name
	p.bucket = strings.TrimSpace(cfg.Bucket)
	p.prefix = normalizePrefix(cfg.Prefix)
	p.store = store
	p.urls = newObjectURLStore(uuid.NewString)
	p.mu.Unlock()

	if oldStore != nil {
		_ = oldStore.Close()
	}
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	p.mu.RLock()
	name := p.name
	p.mu.RUnlock()
	if name == "" {
		name = "gcs"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindFileAPI,
		Name:        name,
		DisplayName: "Google Cloud Storage",
		Description: "Google Cloud Storage FileAPI provider.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	store, err := p.currentStore()
	if err != nil {
		return err
	}
	healthCtx, cancel := p.withTimeout(ctx)
	defer cancel()
	return store.HealthCheck(healthCtx)
}

func (p *Provider) Close() error {
	p.mu.Lock()
	store := p.store
	p.store = nil
	p.urls = newObjectURLStore(uuid.NewString)
	p.mu.Unlock()
	if store == nil {
		return nil
	}
	return store.Close()
}

func (p *Provider) CreateBlob(ctx context.Context, req *proto.CreateBlobRequest) (*proto.FileObjectResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	store, err := p.currentStore()
	if err != nil {
		return nil, err
	}
	data, err := p.materializeParts(ctx, store, req.GetParts(), req.GetOptions().GetEndings())
	if err != nil {
		return nil, err
	}

	contentType := normalizeType(req.GetOptions().GetMimeType())
	id := p.nextObjectID()

	writeCtx, cancel := p.withTimeout(ctx)
	defer cancel()
	attrs, err := store.Write(writeCtx, id, data, contentType, objectMetadata(proto.FileObjectKind_FILE_OBJECT_KIND_BLOB, "", 0))
	if err != nil {
		return nil, err
	}
	return &proto.FileObjectResponse{Object: fileObjectFromAttrs(attrs)}, nil
}

func (p *Provider) CreateFile(ctx context.Context, req *proto.CreateFileRequest) (*proto.FileObjectResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	store, err := p.currentStore()
	if err != nil {
		return nil, err
	}
	data, err := p.materializeParts(ctx, store, req.GetFileBits(), req.GetOptions().GetEndings())
	if err != nil {
		return nil, err
	}

	contentType := normalizeType(req.GetOptions().GetMimeType())
	lastModified := p.resolveLastModified(req.GetOptions().GetLastModified())
	id := p.nextObjectID()

	writeCtx, cancel := p.withTimeout(ctx)
	defer cancel()
	attrs, err := store.Write(writeCtx, id, data, contentType, objectMetadata(proto.FileObjectKind_FILE_OBJECT_KIND_FILE, req.GetFileName(), lastModified))
	if err != nil {
		return nil, err
	}
	return &proto.FileObjectResponse{Object: fileObjectFromAttrs(attrs)}, nil
}

func (p *Provider) Stat(ctx context.Context, req *proto.FileObjectRequest) (*proto.FileObjectResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	store, err := p.currentStore()
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	statCtx, cancel := p.withTimeout(ctx)
	defer cancel()
	attrs, err := store.Stat(statCtx, id)
	if err != nil {
		return nil, err
	}
	return &proto.FileObjectResponse{Object: fileObjectFromAttrs(attrs)}, nil
}

func (p *Provider) Slice(ctx context.Context, req *proto.SliceRequest) (*proto.FileObjectResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	store, err := p.currentStore()
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	statCtx, cancel := p.withTimeout(ctx)
	defer cancel()
	source, err := store.Stat(statCtx, id)
	if err != nil {
		return nil, err
	}

	start, length := normalizeSliceRange(source.Size, req.Start, req.End)
	data := []byte{}
	if length > 0 {
		readCtx, readCancel := p.withTimeout(ctx)
		defer readCancel()
		reader, err := store.OpenRange(readCtx, id, start, length)
		if err != nil {
			return nil, err
		}
		data, err = io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return nil, err
		}
	}

	contentType := req.GetContentType()
	if contentType == "" {
		contentType = source.ContentType
	}
	contentType = normalizeType(contentType)
	newID := p.nextObjectID()

	writeCtx, writeCancel := p.withTimeout(ctx)
	defer writeCancel()
	attrs, err := store.Write(writeCtx, newID, data, contentType, objectMetadata(proto.FileObjectKind_FILE_OBJECT_KIND_BLOB, "", 0))
	if err != nil {
		return nil, err
	}
	return &proto.FileObjectResponse{Object: fileObjectFromAttrs(attrs)}, nil
}

func (p *Provider) ReadBytes(ctx context.Context, req *proto.FileObjectRequest) (*proto.BytesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	store, err := p.currentStore()
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	readCtx, cancel := p.withTimeout(ctx)
	defer cancel()
	data, err := store.ReadAll(readCtx, id)
	if err != nil {
		return nil, err
	}
	return &proto.BytesResponse{Data: data}, nil
}

func (p *Provider) OpenReadStream(req *proto.ReadStreamRequest, stream proto.FileAPI_OpenReadStreamServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	store, err := p.currentStore()
	if err != nil {
		return err
	}
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return status.Error(codes.InvalidArgument, "id is required")
	}

	reader, err := store.OpenRange(stream.Context(), id, 0, -1)
	if err != nil {
		return err
	}
	defer reader.Close()

	buf := make([]byte, streamChunkSize)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			chunk := slices.Clone(buf[:n])
			if err := stream.Send(&proto.ReadChunk{Data: chunk}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (p *Provider) CreateObjectURL(ctx context.Context, req *proto.CreateObjectURLRequest) (*proto.ObjectURLResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	store, err := p.currentStore()
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	statCtx, cancel := p.withTimeout(ctx)
	defer cancel()
	if _, err := store.Stat(statCtx, id); err != nil {
		return nil, err
	}
	return &proto.ObjectURLResponse{Url: p.urlStore().Create(id)}, nil
}

func (p *Provider) ResolveObjectURL(ctx context.Context, req *proto.ObjectURLRequest) (*proto.FileObjectResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	url := strings.TrimSpace(req.GetUrl())
	if url == "" {
		return nil, status.Error(codes.InvalidArgument, "url is required")
	}
	id, ok := p.urlStore().Resolve(url)
	if !ok {
		return nil, status.Error(codes.NotFound, "object URL not found")
	}

	store, err := p.currentStore()
	if err != nil {
		return nil, err
	}
	statCtx, cancel := p.withTimeout(ctx)
	defer cancel()
	attrs, err := store.Stat(statCtx, id)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			p.urlStore().Revoke(url)
		}
		return nil, err
	}
	return &proto.FileObjectResponse{Object: fileObjectFromAttrs(attrs)}, nil
}

func (p *Provider) RevokeObjectURL(_ context.Context, req *proto.ObjectURLRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	url := strings.TrimSpace(req.GetUrl())
	if url == "" {
		return nil, status.Error(codes.InvalidArgument, "url is required")
	}
	p.urlStore().Revoke(url)
	return &emptypb.Empty{}, nil
}

func newGCSStore(ctx context.Context, bucket string) (*gcsStore, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	store := &gcsStore{
		client:     client,
		bucket:     client.Bucket(bucket),
		bucketName: bucket,
	}
	if err := store.HealthCheck(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return store, nil
}

func (s *gcsStore) HealthCheck(ctx context.Context) error {
	_, err := s.bucket.Attrs(ctx)
	return mapStoreErr(err)
}

func (s *gcsStore) Stat(ctx context.Context, id string) (*objectAttrs, error) {
	attrs, err := s.bucket.Object(id).Attrs(ctx)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return objectAttrsFromGCS(id, attrs), nil
}

func (s *gcsStore) Write(ctx context.Context, id string, data []byte, contentType string, metadata map[string]string) (*objectAttrs, error) {
	object := s.bucket.Object(id)
	writer := object.NewWriter(ctx)
	writer.ContentType = contentType
	writer.Metadata = metadata
	if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		_ = writer.Close()
		return nil, mapStoreErr(err)
	}
	if err := writer.Close(); err != nil {
		return nil, mapStoreErr(err)
	}
	attrs, err := object.Attrs(ctx)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return objectAttrsFromGCS(id, attrs), nil
}

func (s *gcsStore) ReadAll(ctx context.Context, id string) ([]byte, error) {
	reader, err := s.bucket.Object(id).NewReader(ctx)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *gcsStore) OpenRange(ctx context.Context, id string, offset, length int64) (io.ReadCloser, error) {
	reader, err := s.bucket.Object(id).NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return reader, nil
}

func (s *gcsStore) Close() error {
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

func newObjectURLStore(newToken func() string) *objectURLStore {
	return &objectURLStore{
		values:   make(map[string]string),
		newToken: newToken,
	}
}

func (s *objectURLStore) Create(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		url := objectURLPrefix + s.newToken()
		if _, exists := s.values[url]; exists {
			continue
		}
		s.values[url] = id
		return url
	}
}

func (s *objectURLStore) Resolve(url string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.values[url]
	return id, ok
}

func (s *objectURLStore) Revoke(url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[url]; !ok {
		return false
	}
	delete(s.values, url)
	return true
}

func (p *Provider) initDefaults() {
	if p.newID == nil {
		p.newID = uuid.NewString
	}
	if p.now == nil {
		p.now = time.Now
	}
	if p.timeout == 0 {
		p.timeout = defaultTimeout
	}
	if p.urls == nil {
		p.urls = newObjectURLStore(uuid.NewString)
	}
}

func (p *Provider) currentStore() (objectStore, error) {
	p.initDefaults()
	p.mu.RLock()
	store := p.store
	p.mu.RUnlock()
	if store == nil {
		return nil, status.Error(codes.FailedPrecondition, "provider is not configured")
	}
	return store, nil
}

func (p *Provider) urlStore() *objectURLStore {
	p.initDefaults()
	return p.urls
}

func (p *Provider) nextObjectID() string {
	p.initDefaults()
	p.mu.RLock()
	prefix := p.prefix
	p.mu.RUnlock()
	return prefix + p.newID()
}

func (p *Provider) resolveLastModified(lastModified int64) int64 {
	p.initDefaults()
	if lastModified >= 0 {
		return lastModified
	}
	return p.now().UnixMilli()
}

func (p *Provider) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	p.initDefaults()
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, p.timeout)
}

func (p *Provider) materializeParts(ctx context.Context, store objectStore, parts []*proto.BlobPart, endings proto.LineEndings) ([]byte, error) {
	var out bytes.Buffer
	for _, part := range parts {
		switch kind := part.GetKind().(type) {
		case *proto.BlobPart_StringData:
			out.WriteString(applyLineEndings(kind.StringData, endings))
		case *proto.BlobPart_BytesData:
			out.Write(kind.BytesData)
		case *proto.BlobPart_BlobId:
			readCtx, cancel := p.withTimeout(ctx)
			data, err := store.ReadAll(readCtx, strings.TrimSpace(kind.BlobId))
			cancel()
			if err != nil {
				return nil, err
			}
			out.Write(data)
		default:
			return nil, status.Error(codes.InvalidArgument, "blob part kind is required")
		}
	}
	return out.Bytes(), nil
}

func applyLineEndings(value string, endings proto.LineEndings) string {
	if endings != proto.LineEndings_LINE_ENDINGS_NATIVE {
		return value
	}
	normalized := strings.ReplaceAll(value, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	if runtime.GOOS == "windows" {
		return strings.ReplaceAll(normalized, "\n", "\r\n")
	}
	return normalized
}

func normalizePrefix(prefix string) string {
	trimmed := strings.Trim(strings.TrimSpace(prefix), "/")
	if trimmed == "" {
		return ""
	}
	return trimmed + "/"
}

func normalizeSliceRange(size int64, start, end *int64) (int64, int64) {
	rangeStart := int64(0)
	if start != nil {
		rangeStart = *start
	}
	rangeEnd := size
	if end != nil {
		rangeEnd = *end
	}

	if rangeStart < 0 {
		rangeStart += size
	}
	if rangeEnd < 0 {
		rangeEnd += size
	}
	if rangeStart < 0 {
		rangeStart = 0
	}
	if rangeEnd < 0 {
		rangeEnd = 0
	}
	if rangeStart > size {
		rangeStart = size
	}
	if rangeEnd > size {
		rangeEnd = size
	}
	if rangeEnd < rangeStart {
		rangeEnd = rangeStart
	}
	return rangeStart, rangeEnd - rangeStart
}

func objectMetadata(kind proto.FileObjectKind, name string, lastModified int64) map[string]string {
	metadata := map[string]string{
		metaKind: kindMetadataValue(kind),
	}
	if kind == proto.FileObjectKind_FILE_OBJECT_KIND_FILE {
		metadata[metaLastModified] = strconv.FormatInt(lastModified, 10)
	}
	if name != "" {
		metadata[metaName] = name
	}
	return metadata
}

func kindMetadataValue(kind proto.FileObjectKind) string {
	switch kind {
	case proto.FileObjectKind_FILE_OBJECT_KIND_FILE:
		return "file"
	default:
		return "blob"
	}
}

func fileObjectFromAttrs(attrs *objectAttrs) *proto.FileObject {
	if attrs == nil {
		return nil
	}
	kind := proto.FileObjectKind_FILE_OBJECT_KIND_BLOB
	switch attrs.Metadata[metaKind] {
	case "file":
		kind = proto.FileObjectKind_FILE_OBJECT_KIND_FILE
	}
	return &proto.FileObject{
		Id:           attrs.ID,
		Kind:         kind,
		Size:         attrs.Size,
		Type:         normalizeType(attrs.ContentType),
		Name:         attrs.Metadata[metaName],
		LastModified: fileLastModified(attrs),
	}
}

func fileLastModified(attrs *objectAttrs) int64 {
	if attrs == nil {
		return 0
	}
	if attrs.Metadata[metaKind] != "file" {
		return 0
	}
	if raw := attrs.Metadata[metaLastModified]; raw != "" {
		if value, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return value
		}
	}
	return attrs.Updated.UnixMilli()
}

func objectAttrsFromGCS(id string, attrs *storage.ObjectAttrs) *objectAttrs {
	metadata := make(map[string]string, len(attrs.Metadata))
	for key, value := range attrs.Metadata {
		metadata[key] = value
	}
	return &objectAttrs{
		ID:          id,
		Size:        attrs.Size,
		ContentType: attrs.ContentType,
		Metadata:    metadata,
		Updated:     attrs.Updated,
	}
}

func mapStoreErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return status.Error(codes.NotFound, "file object not found")
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound, codes.PermissionDenied:
			return err
		}
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case 403:
			return status.Error(codes.PermissionDenied, apiErr.Message)
		case 404:
			return status.Error(codes.NotFound, apiErr.Message)
		}
	}
	return err
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
