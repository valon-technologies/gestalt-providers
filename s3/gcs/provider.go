package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion   = "0.0.1-alpha.1"
	defaultPresignTTL = 15 * time.Minute
	maxPresignTTL     = 7 * 24 * time.Hour
)

var errNotConfigured = errors.New("gcs: not configured")

type config struct {
	Bucket      string `yaml:"bucket"`
	KeyPrefix   string `yaml:"keyPrefix"`
	UserProject string `yaml:"userProject"`
}

type configuredProvider struct {
	client      *storage.Client
	bucket      *storage.BucketHandle
	bucketName  string
	keyPrefix   string
	userProject string
}

type Provider struct {
	mu     sync.RWMutex
	name   string
	cfg    config
	client *storage.Client
	bucket *storage.BucketHandle
	now    func() time.Time
}

func New() *Provider {
	return &Provider{now: time.Now}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	var cfg config
	data, err := yaml.Marshal(raw)
	if err != nil {
		return fmt.Errorf("gcs: marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("gcs: decode config: %w", err)
	}

	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	cfg.KeyPrefix = normalizeKeyPrefix(cfg.KeyPrefix)
	cfg.UserProject = strings.TrimSpace(cfg.UserProject)
	if cfg.Bucket == "" {
		return fmt.Errorf("gcs: bucket is required")
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("gcs: create client: %w", err)
	}
	bucket := client.Bucket(cfg.Bucket)
	if cfg.UserProject != "" {
		bucket = bucket.UserProject(cfg.UserProject)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		_ = p.client.Close()
	}
	p.name = name
	p.cfg = cfg
	p.client = client
	p.bucket = bucket
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	name := strings.TrimSpace(p.name)
	if name == "" {
		name = "gcs"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindS3,
		Name:        name,
		DisplayName: "Google Cloud Storage",
		Description: "S3 provider backed by Google Cloud Storage with generation-aware object versions.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg, err := p.configured()
	if err != nil {
		return err
	}
	healthCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := cfg.bucket.Attrs(healthCtx); err != nil {
		if isPermissionDenied(err) {
			return nil
		}
		return toStatusError(fmt.Errorf("gcs: healthcheck bucket %s: %w", cfg.bucketName, err))
	}
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	client := p.client
	p.client = nil
	p.bucket = nil
	if client == nil {
		return nil
	}
	return client.Close()
}

func (p *Provider) HeadObject(ctx context.Context, ref gestalt.ObjectRef) (gestalt.ObjectMeta, error) {
	ref, err := validateObjectRef(ref, "ref")
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	cfg, err := p.configured()
	if err != nil {
		return gestalt.ObjectMeta{}, toStatusError(err)
	}
	obj, err := objectHandle(cfg, ref)
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return gestalt.ObjectMeta{}, toStatusError(fmt.Errorf("gcs: head object %s/%s: %w", cfg.bucketName, backendKey(cfg.keyPrefix, ref.Key), err))
	}
	return objectMetaFromAttrs(cfg.keyPrefix, attrs), nil
}

func (p *Provider) ReadObject(ctx context.Context, req gestalt.ReadRequest) (gestalt.ReadResult, error) {
	ref := req.Ref
	ref, err := validateObjectRef(ref, "ref")
	if err != nil {
		return gestalt.ReadResult{}, err
	}
	if err := validateReadRequest(req); err != nil {
		return gestalt.ReadResult{}, err
	}
	cfg, err := p.configured()
	if err != nil {
		return gestalt.ReadResult{}, toStatusError(err)
	}
	obj, err := objectHandle(cfg, ref)
	if err != nil {
		return gestalt.ReadResult{}, err
	}
	obj = obj.ReadCompressed(true)
	offset, length, ranged, err := rangeParams(req.Range)
	if err != nil {
		return gestalt.ReadResult{}, err
	}
	var reader *storage.Reader
	if ranged {
		reader, err = obj.NewRangeReader(ctx, offset, length)
	} else {
		reader, err = obj.NewReader(ctx)
	}
	if err != nil {
		return gestalt.ReadResult{}, toStatusError(fmt.Errorf("gcs: read object %s/%s: %w", cfg.bucketName, backendKey(cfg.keyPrefix, ref.Key), err))
	}
	metaRef := ref
	if reader.Attrs.Generation > 0 {
		metaRef.VersionID = strconv.FormatInt(reader.Attrs.Generation, 10)
	}
	meta, err := p.HeadObject(ctx, metaRef)
	if err != nil {
		if !statusCodeIs(err, gestalt.CodeNotFound) {
			_ = reader.Close()
			return gestalt.ReadResult{}, err
		}
		meta = objectMetaFromReaderAttrs(ref.Key, reader.Attrs)
	}
	return gestalt.ReadResult{Meta: meta, Body: reader}, nil
}

func (p *Provider) WriteObject(ctx context.Context, req gestalt.WriteRequest) (gestalt.ObjectMeta, error) {
	ref, err := validateObjectRef(req.Ref, "ref")
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	cfg, err := p.configured()
	if err != nil {
		return gestalt.ObjectMeta{}, toStatusError(err)
	}
	obj, err := writeObjectHandle(cfg, ref, &req)
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	body := req.Body
	if body == nil {
		body = strings.NewReader("")
	}
	writer := obj.NewWriter(ctx)
	writer.ContentType = req.ContentType
	writer.CacheControl = req.CacheControl
	writer.ContentDisposition = req.ContentDisposition
	writer.ContentEncoding = req.ContentEncoding
	writer.ContentLanguage = req.ContentLanguage
	writer.Metadata = cloneStringMap(req.Metadata)
	if _, err := io.Copy(writer, body); err != nil {
		_ = writer.CloseWithError(err)
		return gestalt.ObjectMeta{}, toStatusError(fmt.Errorf("gcs: write object %s/%s: %w", cfg.bucketName, backendKey(cfg.keyPrefix, ref.Key), err))
	}
	if err := writer.Close(); err != nil {
		return gestalt.ObjectMeta{}, toStatusError(fmt.Errorf("gcs: write object %s/%s: %w", cfg.bucketName, backendKey(cfg.keyPrefix, ref.Key), err))
	}
	if attrs := writer.Attrs(); attrs != nil && attrs.Generation > 0 {
		return objectMetaFromAttrs(cfg.keyPrefix, attrs), nil
	}
	return p.HeadObject(ctx, gestalt.ObjectRef{Key: ref.Key})
}

func (p *Provider) DeleteObject(ctx context.Context, ref gestalt.ObjectRef) error {
	ref, err := validateObjectRef(ref, "ref")
	if err != nil {
		return err
	}
	cfg, err := p.configured()
	if err != nil {
		return toStatusError(err)
	}
	obj, err := objectHandle(cfg, ref)
	if err != nil {
		return err
	}
	if err := obj.Delete(ctx); err != nil {
		return toStatusError(fmt.Errorf("gcs: delete object %s/%s: %w", cfg.bucketName, backendKey(cfg.keyPrefix, ref.Key), err))
	}
	return nil
}

func (p *Provider) ListObjects(ctx context.Context, opts gestalt.ListRequest) (gestalt.ListPage, error) {
	cfg, err := p.configured()
	if err != nil {
		return gestalt.ListPage{}, toStatusError(err)
	}
	query := &storage.Query{
		Prefix:    backendKey(cfg.keyPrefix, opts.Prefix),
		Delimiter: opts.Delimiter,
	}
	if opts.StartAfter != "" {
		query.StartOffset = backendKey(cfg.keyPrefix, opts.StartAfter)
	}
	pageSize := int(opts.MaxKeys)
	if pageSize <= 0 {
		pageSize = 1000
	}
	it := cfg.bucket.Objects(ctx, query)
	var attrs []*storage.ObjectAttrs
	nextToken, err := iterator.NewPager(it, pageSize, opts.ContinuationToken).NextPage(&attrs)
	if err != nil {
		return gestalt.ListPage{}, toStatusError(fmt.Errorf("gcs: list objects %s/%s: %w", cfg.bucketName, query.Prefix, err))
	}
	page := gestalt.ListPage{
		NextContinuationToken: nextToken,
		HasMore:               nextToken != "",
	}
	appendListAttrs(&page, cfg.keyPrefix, opts.StartAfter, attrs)
	return page, nil
}

func (p *Provider) CopyObject(ctx context.Context, req gestalt.CopyRequest) (gestalt.ObjectMeta, error) {
	source := req.Source
	source, err := validateObjectRef(source, "source")
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	destination := req.Destination
	destination, err = validateObjectRef(destination, "destination")
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	if err := validateCopyRequest(req); err != nil {
		return gestalt.ObjectMeta{}, err
	}
	cfg, err := p.configured()
	if err != nil {
		return gestalt.ObjectMeta{}, toStatusError(err)
	}
	src, err := objectHandle(cfg, source)
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	dst, err := destinationCopyHandle(cfg, destination)
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	attrs, err := dst.CopierFrom(src).Run(ctx)
	if err != nil {
		return gestalt.ObjectMeta{}, toStatusError(fmt.Errorf("gcs: copy object %s/%s to %s/%s: %w", cfg.bucketName, backendKey(cfg.keyPrefix, source.Key), cfg.bucketName, backendKey(cfg.keyPrefix, destination.Key), err))
	}
	return objectMetaFromAttrs(cfg.keyPrefix, attrs), nil
}

func (p *Provider) PresignObject(ctx context.Context, req gestalt.PresignRequest) (gestalt.PresignResult, error) {
	_ = ctx
	ref := req.Ref
	ref, err := validateObjectRef(ref, "ref")
	if err != nil {
		return gestalt.PresignResult{}, err
	}
	cfg, err := p.configured()
	if err != nil {
		return gestalt.PresignResult{}, toStatusError(err)
	}
	method := gestalt.PresignMethodGet
	expires := defaultPresignTTL
	contentType := ""
	contentDisposition := ""
	headers := map[string]string{}
	if req.Method != "" {
		method = req.Method
	}
	if req.Expires < 0 {
		return gestalt.PresignResult{}, gestalt.InvalidArgument("gcs: expires must be >= 0")
	}
	if req.Expires > 0 {
		expires = req.Expires
	}
	contentType = req.ContentType
	contentDisposition = req.ContentDisposition
	headers = clonePresignHeaders(req.Headers)
	signedOpts := &storage.SignedURLOptions{
		Method:          string(method),
		Expires:         p.now().Add(expires),
		Scheme:          storage.SigningSchemeV4,
		QueryParameters: url.Values{},
		ContentType:     "",
		Headers:         nil,
	}
	if expires > maxPresignTTL {
		return gestalt.PresignResult{}, gestalt.InvalidArgument("gcs: expires must be <= 7 days for V4 signed URLs")
	}
	switch method {
	case gestalt.PresignMethodGet, gestalt.PresignMethodHead, gestalt.PresignMethodDelete:
		if ref.VersionID != "" {
			if _, err := parseGeneration(ref.VersionID, "ref.versionId"); err != nil {
				return gestalt.PresignResult{}, err
			}
			signedOpts.QueryParameters.Set("generation", ref.VersionID)
		}
		if contentType != "" {
			signedOpts.QueryParameters.Set("response-content-type", contentType)
		}
		if contentDisposition != "" {
			signedOpts.QueryParameters.Set("response-content-disposition", contentDisposition)
		}
	case gestalt.PresignMethodPut:
		if contentType != "" {
			signedOpts.ContentType = contentType
			headers = setHeader(headers, "Content-Type", contentType)
		}
		if contentDisposition != "" {
			headers = setHeader(headers, "Content-Disposition", contentDisposition)
		}
		if ref.VersionID != "" {
			if _, err := parseGeneration(ref.VersionID, "ref.versionId"); err != nil {
				return gestalt.PresignResult{}, err
			}
			headers = setHeader(headers, "x-goog-if-generation-match", ref.VersionID)
		}
	default:
		return gestalt.PresignResult{}, gestalt.InvalidArgument(fmt.Sprintf("gcs: unsupported presign method %q", method))
	}
	signedOpts.Headers = signedURLHeaders(headers, signedOpts.ContentType)
	urlText, err := cfg.bucket.SignedURL(backendKey(cfg.keyPrefix, ref.Key), signedOpts)
	if err != nil {
		return gestalt.PresignResult{}, toStatusError(fmt.Errorf("gcs: presign object %s/%s: %w", cfg.bucketName, backendKey(cfg.keyPrefix, ref.Key), err))
	}
	return gestalt.PresignResult{
		URL:       urlText,
		Method:    method,
		ExpiresAt: signedOpts.Expires,
		Headers:   headers,
	}, nil
}

func (p *Provider) configured() (configuredProvider, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.client == nil || p.bucket == nil {
		return configuredProvider{}, errNotConfigured
	}
	return configuredProvider{
		client:      p.client,
		bucket:      p.bucket,
		bucketName:  p.cfg.Bucket,
		keyPrefix:   p.cfg.KeyPrefix,
		userProject: p.cfg.UserProject,
	}, nil
}

func validateObjectRef(ref gestalt.ObjectRef, field string) (gestalt.ObjectRef, error) {
	if ref.Key == "" {
		return gestalt.ObjectRef{}, gestalt.InvalidArgument(fmt.Sprintf("%s.key is required", field))
	}
	if ref.VersionID != "" {
		if _, err := parseGeneration(ref.VersionID, field+".versionId"); err != nil {
			return gestalt.ObjectRef{}, err
		}
	}
	return ref, nil
}

func objectHandle(cfg configuredProvider, ref gestalt.ObjectRef) (*storage.ObjectHandle, error) {
	obj := cfg.bucket.Object(backendKey(cfg.keyPrefix, ref.Key))
	if ref.VersionID == "" {
		return obj, nil
	}
	generation, err := parseGeneration(ref.VersionID, "ref.versionId")
	if err != nil {
		return nil, err
	}
	return obj.Generation(generation), nil
}

func writeObjectHandle(cfg configuredProvider, ref gestalt.ObjectRef, opts *gestalt.WriteRequest) (*storage.ObjectHandle, error) {
	obj := cfg.bucket.Object(backendKey(cfg.keyPrefix, ref.Key))
	if opts != nil {
		if opts.IfMatch != "" {
			return nil, unsupportedCondition("write", "ifMatch")
		}
		if opts.IfNoneMatch != "" && opts.IfNoneMatch != "*" {
			return nil, unsupportedCondition("write", "ifNoneMatch")
		}
	}
	hasGeneration := ref.VersionID != ""
	createOnly := opts != nil && opts.IfNoneMatch == "*"
	if hasGeneration && createOnly {
		return nil, gestalt.InvalidArgument("gcs: cannot combine ref.versionId with ifNoneMatch \"*\"")
	}
	switch {
	case createOnly:
		return obj.If(storage.Conditions{DoesNotExist: true}), nil
	case hasGeneration:
		generation, err := parseGeneration(ref.VersionID, "ref.versionId")
		if err != nil {
			return nil, err
		}
		return obj.If(storage.Conditions{GenerationMatch: generation}), nil
	default:
		return obj, nil
	}
}

func destinationCopyHandle(cfg configuredProvider, ref gestalt.ObjectRef) (*storage.ObjectHandle, error) {
	obj := cfg.bucket.Object(backendKey(cfg.keyPrefix, ref.Key))
	if ref.VersionID == "" {
		return obj, nil
	}
	generation, err := parseGeneration(ref.VersionID, "destination.versionId")
	if err != nil {
		return nil, err
	}
	return obj.If(storage.Conditions{GenerationMatch: generation}), nil
}

func validateReadRequest(req gestalt.ReadRequest) error {
	if req.IfMatch != "" {
		return unsupportedCondition("read", "ifMatch")
	}
	if req.IfNoneMatch != "" {
		return unsupportedCondition("read", "ifNoneMatch")
	}
	if req.IfModifiedSince != nil {
		return unsupportedCondition("read", "ifModifiedSince")
	}
	if req.IfUnmodifiedSince != nil {
		return unsupportedCondition("read", "ifUnmodifiedSince")
	}
	return nil
}

func validateCopyRequest(req gestalt.CopyRequest) error {
	if req.IfMatch != "" {
		return unsupportedCondition("copy", "ifMatch")
	}
	if req.IfNoneMatch != "" {
		return unsupportedCondition("copy", "ifNoneMatch")
	}
	return nil
}

func unsupportedCondition(operation, field string) error {
	return gestalt.InvalidArgument(fmt.Sprintf("gcs: %s %s is not supported by the generation-only S3 surface", operation, field))
}

func rangeParams(r *gestalt.ByteRange) (offset, length int64, ranged bool, err error) {
	if r == nil {
		return 0, 0, false, nil
	}
	var startSet, endSet bool
	var start, end int64
	if r.Start != nil {
		startSet = true
		start = *r.Start
	}
	if r.End != nil {
		endSet = true
		end = *r.End
	}
	if startSet && start < 0 {
		return 0, 0, false, gestalt.OutOfRange("gcs: invalid range")
	}
	if endSet && end < 0 {
		return 0, 0, false, gestalt.OutOfRange("gcs: invalid range")
	}
	if startSet && endSet && start > end {
		return 0, 0, false, gestalt.OutOfRange("gcs: invalid range")
	}
	switch {
	case startSet && endSet:
		return start, end - start + 1, true, nil
	case startSet:
		return start, -1, true, nil
	case endSet:
		return 0, end + 1, true, nil
	default:
		return 0, 0, false, nil
	}
}

func parseGeneration(value, field string) (int64, error) {
	generation, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || generation <= 0 {
		return 0, gestalt.InvalidArgument(fmt.Sprintf("gcs: %s must be a positive decimal generation", field))
	}
	return generation, nil
}

func normalizeKeyPrefix(value string) string {
	value = strings.Trim(strings.TrimSpace(value), "/")
	if value == "" {
		return ""
	}
	return value + "/"
}

func backendKey(keyPrefix, key string) string {
	return keyPrefix + key
}

func logicalKey(keyPrefix, key string) (string, bool) {
	if keyPrefix == "" {
		return key, key != ""
	}
	logical, ok := strings.CutPrefix(key, keyPrefix)
	if !ok || logical == "" {
		return "", false
	}
	return logical, true
}

func objectMetaFromAttrs(keyPrefix string, attrs *storage.ObjectAttrs) gestalt.ObjectMeta {
	if attrs == nil {
		return gestalt.ObjectMeta{}
	}
	key, _ := logicalKey(keyPrefix, attrs.Name)
	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Key: key,
		},
		ETag:         attrs.Etag,
		Size:         attrs.Size,
		ContentType:  attrs.ContentType,
		Metadata:     cloneStringMap(attrs.Metadata),
		StorageClass: attrs.StorageClass,
	}
	if attrs.Generation > 0 {
		meta.Ref.VersionID = strconv.FormatInt(attrs.Generation, 10)
	}
	if !attrs.Updated.IsZero() {
		meta.LastModified = attrs.Updated.UTC()
	} else if !attrs.Created.IsZero() {
		meta.LastModified = attrs.Created.UTC()
	}
	return meta
}

func objectMetaFromListAttrs(keyPrefix string, attrs *storage.ObjectAttrs) (gestalt.ObjectMeta, bool) {
	if attrs == nil {
		return gestalt.ObjectMeta{}, false
	}
	key, ok := logicalKey(keyPrefix, attrs.Name)
	if !ok {
		return gestalt.ObjectMeta{}, false
	}
	meta := objectMetaFromAttrs(keyPrefix, attrs)
	meta.Ref.Key = key
	return meta, true
}

func objectMetaFromReaderAttrs(key string, attrs storage.ReaderObjectAttrs) gestalt.ObjectMeta {
	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Key: key,
		},
		Size:        attrs.Size,
		ContentType: attrs.ContentType,
	}
	if attrs.Generation > 0 {
		meta.Ref.VersionID = strconv.FormatInt(attrs.Generation, 10)
	}
	if !attrs.LastModified.IsZero() {
		meta.LastModified = attrs.LastModified.UTC()
	}
	return meta
}

func appendListAttrs(page *gestalt.ListPage, keyPrefix, startAfter string, attrs []*storage.ObjectAttrs) {
	for _, attr := range attrs {
		if attr.Prefix != "" {
			prefix, ok := logicalKey(keyPrefix, attr.Prefix)
			if ok && (startAfter == "" || prefix > startAfter) {
				page.CommonPrefixes = append(page.CommonPrefixes, prefix)
			}
			continue
		}
		meta, ok := objectMetaFromListAttrs(keyPrefix, attr)
		if ok && (startAfter == "" || meta.Ref.Key > startAfter) {
			page.Objects = append(page.Objects, meta)
		}
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func clonePresignHeaders(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		if strings.EqualFold(key, "host") {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func setHeader(headers map[string]string, key, value string) map[string]string {
	if value == "" {
		return headers
	}
	if headers == nil {
		headers = make(map[string]string, 1)
	}
	for existing := range headers {
		if strings.EqualFold(existing, key) {
			delete(headers, existing)
		}
	}
	headers[key] = value
	return headers
}

func signedURLHeaders(headers map[string]string, contentType string) []string {
	out := make([]string, 0, len(headers))
	for key, value := range headers {
		if strings.EqualFold(key, "host") || (contentType != "" && strings.EqualFold(key, "content-type")) {
			continue
		}
		out = append(out, key+":"+value)
	}
	return out
}

func toStatusError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := gestalt.StatusCodeOf(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return gestalt.Canceled(err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return gestalt.Canceled(err.Error())
	case errors.Is(err, errNotConfigured):
		return gestalt.FailedPrecondition(err.Error())
	case isNotFound(err):
		return gestalt.NotFound(err.Error())
	case isNotModified(err), isPreconditionFailed(err):
		return gestalt.FailedPrecondition(err.Error())
	case isInvalidRange(err):
		return gestalt.OutOfRange(err.Error())
	case isPermissionDenied(err):
		return gestalt.PermissionDenied(err.Error())
	case isSigningPrecondition(err):
		return gestalt.FailedPrecondition(err.Error())
	default:
		return gestalt.Internal(err.Error())
	}
}

func statusCodeIs(err error, want gestalt.StatusCode) bool {
	code, ok := gestalt.StatusCodeOf(err)
	return ok && code == want
}

func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist) || googleStatus(err) == http.StatusNotFound || status.Code(err) == codes.NotFound
}

func isPreconditionFailed(err error) bool {
	return googleStatus(err) == http.StatusPreconditionFailed || status.Code(err) == codes.FailedPrecondition
}

func isNotModified(err error) bool {
	return googleStatus(err) == http.StatusNotModified
}

func isInvalidRange(err error) bool {
	return googleStatus(err) == http.StatusRequestedRangeNotSatisfiable || status.Code(err) == codes.OutOfRange
}

func isPermissionDenied(err error) bool {
	return googleStatus(err) == http.StatusForbidden || status.Code(err) == codes.PermissionDenied
}

func isSigningPrecondition(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "signbytes") ||
		strings.Contains(message, "private key") ||
		strings.Contains(message, "googleaccessid") ||
		strings.Contains(message, "service account")
}

func googleStatus(err error) int {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return 0
}

var _ gestalt.S3Provider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
