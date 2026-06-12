package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/s3"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion        = "0.0.1-alpha.1"
	payloadSigningAuto     = "auto"
	payloadSigningSigned   = "signed"
	signingMiddlewareID    = "Signing"
	acceptEncodingHeader   = "Accept-Encoding"
	amzSDKInvocationID     = "Amz-Sdk-Invocation-Id"
	amzSDKRequestHeader    = "Amz-Sdk-Request"
	acceptEncodingIdentity = "identity"
)

var (
	errNotConfigured                 = errors.New("s3: not configured")
	errMultipartUploadRequired       = errors.New("s3: multipart upload required")
	errMultipartCopyRequired         = errors.New("s3: multipart copy required")
	maxSingleRequestObjectSize int64 = 5 * 1024 * 1024 * 1024
)

type config struct {
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	KeyPrefix       string `yaml:"keyPrefix"`
	Endpoint        string `yaml:"endpoint"`
	ForcePathStyle  bool   `yaml:"forcePathStyle"`
	AccessKeyID     string `yaml:"accessKeyId"`
	SecretAccessKey string `yaml:"secretAccessKey"`
	SessionToken    string `yaml:"sessionToken"`
	PayloadSigning  string `yaml:"payloadSigning"`
}

type configuredProvider struct {
	client    *s3sdk.Client
	presigner *s3sdk.PresignClient
	bucket    string
	keyPrefix string
}

type Provider struct {
	mu        sync.RWMutex
	name      string
	cfg       config
	client    *s3sdk.Client
	presigner *s3sdk.PresignClient
	now       func() time.Time
}

func New() *Provider {
	return &Provider{now: time.Now}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	var cfg config
	data, err := yaml.Marshal(raw)
	if err != nil {
		return fmt.Errorf("s3: marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("s3: decode config: %w", err)
	}

	cfg.Region = strings.TrimSpace(cfg.Region)
	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	cfg.KeyPrefix = normalizeKeyPrefix(cfg.KeyPrefix)
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.AccessKeyID = strings.TrimSpace(cfg.AccessKeyID)
	cfg.SecretAccessKey = strings.TrimSpace(cfg.SecretAccessKey)
	cfg.SessionToken = strings.TrimSpace(cfg.SessionToken)
	cfg.PayloadSigning = strings.ToLower(strings.TrimSpace(cfg.PayloadSigning))

	switch {
	case cfg.Region == "":
		return fmt.Errorf("s3: region is required")
	case cfg.Bucket == "":
		return fmt.Errorf("s3: bucket is required")
	case (cfg.AccessKeyID == "") != (cfg.SecretAccessKey == ""):
		return fmt.Errorf("s3: accessKeyId and secretAccessKey must be provided together")
	}
	switch cfg.PayloadSigning {
	case "", payloadSigningAuto:
		cfg.PayloadSigning = payloadSigningAuto
	case payloadSigningSigned:
	default:
		return fmt.Errorf("s3: payloadSigning must be %q or %q", payloadSigningAuto, payloadSigningSigned)
	}

	client, presigner, err := buildS3Client(ctx, cfg)
	if err != nil {
		return fmt.Errorf("s3: create client: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.name = name
	p.cfg = cfg
	p.client = client
	p.presigner = presigner
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	name := strings.TrimSpace(p.name)
	if name == "" {
		name = "s3"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindS3,
		Name:        name,
		DisplayName: "S3",
		Description: "S3 provider for AWS S3, GCS XML interoperability, MinIO, and similar backends.",
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
	_, err = cfg.client.HeadBucket(healthCtx, &s3sdk.HeadBucketInput{Bucket: aws.String(cfg.bucket)})
	if err != nil {
		if isAccessDenied(err) {
			return nil
		}
		return fmt.Errorf("s3: healthcheck: %w", err)
	}
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client = nil
	p.presigner = nil
	return nil
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
	meta, err := p.headObject(ctx, cfg, ref)
	if err != nil {
		return gestalt.ObjectMeta{}, toStatusError(err)
	}
	return meta, nil
}

func (p *Provider) ReadObject(ctx context.Context, req gestalt.ReadRequest) (gestalt.ReadResult, error) {
	ref := req.Ref
	ref, err := validateObjectRef(ref, "ref")
	if err != nil {
		return gestalt.ReadResult{}, err
	}

	cfg, err := p.configured()
	if err != nil {
		return gestalt.ReadResult{}, toStatusError(err)
	}

	key := backendKey(cfg.keyPrefix, ref.Key)
	input := &s3sdk.GetObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(key),
	}
	if ref.VersionID != "" {
		input.VersionId = aws.String(ref.VersionID)
	}
	if req.IfMatch != "" {
		input.IfMatch = aws.String(req.IfMatch)
	}
	if req.IfNoneMatch != "" {
		input.IfNoneMatch = aws.String(req.IfNoneMatch)
	}
	if req.IfModifiedSince != nil {
		input.IfModifiedSince = req.IfModifiedSince
	}
	if req.IfUnmodifiedSince != nil {
		input.IfUnmodifiedSince = req.IfUnmodifiedSince
	}
	rangeHeader, err := byteRangeHeader(req.Range)
	if err != nil {
		return gestalt.ReadResult{}, err
	}
	if rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}

	out, err := cfg.client.GetObject(ctx, input)
	if err != nil {
		return gestalt.ReadResult{}, toStatusError(fmt.Errorf("s3: get object %s/%s: %w", cfg.bucket, key, err))
	}
	return gestalt.ReadResult{Meta: objectMetaFromGet(ref, out), Body: out.Body}, nil
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
	meta, err := p.writeObject(ctx, cfg, ref, &req, req.Body)
	return meta, toStatusError(err)
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
	key := backendKey(cfg.keyPrefix, ref.Key)
	input := &s3sdk.DeleteObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(key),
	}
	if ref.VersionID != "" {
		input.VersionId = aws.String(ref.VersionID)
	}
	if _, err := cfg.client.DeleteObject(ctx, input); err != nil {
		return toStatusError(fmt.Errorf("s3: delete object %s/%s: %w", cfg.bucket, key, err))
	}
	return nil
}

func (p *Provider) ListObjects(ctx context.Context, opts gestalt.ListRequest) (gestalt.ListPage, error) {
	cfg, err := p.configured()
	if err != nil {
		return gestalt.ListPage{}, toStatusError(err)
	}
	input := &s3sdk.ListObjectsV2Input{
		Bucket: aws.String(cfg.bucket),
	}
	if cfg.keyPrefix != "" || opts.Prefix != "" {
		input.Prefix = aws.String(backendKey(cfg.keyPrefix, opts.Prefix))
	}
	if opts.Delimiter != "" {
		input.Delimiter = aws.String(opts.Delimiter)
	}
	if opts.ContinuationToken != "" {
		input.ContinuationToken = aws.String(opts.ContinuationToken)
	}
	if opts.StartAfter != "" {
		input.StartAfter = aws.String(backendKey(cfg.keyPrefix, opts.StartAfter))
	}
	if opts.MaxKeys > 0 {
		input.MaxKeys = aws.Int32(opts.MaxKeys)
	}

	out, err := cfg.client.ListObjectsV2(ctx, input)
	if err != nil {
		return gestalt.ListPage{}, toStatusError(fmt.Errorf("s3: list objects in %s: %w", cfg.bucket, err))
	}

	page := gestalt.ListPage{
		CommonPrefixes:        make([]string, 0, len(out.CommonPrefixes)),
		NextContinuationToken: aws.ToString(out.NextContinuationToken),
		HasMore:               aws.ToBool(out.IsTruncated),
		Objects:               make([]gestalt.ObjectMeta, 0, len(out.Contents)),
	}
	for _, prefix := range out.CommonPrefixes {
		if logicalPrefix, ok := logicalKey(cfg.keyPrefix, aws.ToString(prefix.Prefix)); ok {
			page.CommonPrefixes = append(page.CommonPrefixes, logicalPrefix)
		}
	}
	for _, object := range out.Contents {
		if meta, ok := objectMetaFromList(cfg.keyPrefix, object); ok {
			page.Objects = append(page.Objects, meta)
		}
	}
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

	cfg, err := p.configured()
	if err != nil {
		return gestalt.ObjectMeta{}, toStatusError(err)
	}
	sourceHead, err := p.headObject(ctx, cfg, source)
	if err != nil {
		return gestalt.ObjectMeta{}, toStatusError(err)
	}
	if sourceHead.Size > maxSingleRequestObjectSize {
		return gestalt.ObjectMeta{}, toStatusError(fmt.Errorf("%w: object size %d exceeds single-request copy limit %d", errMultipartCopyRequired, sourceHead.Size, maxSingleRequestObjectSize))
	}
	sourceBackendRef := gestalt.ObjectRef{
		Key:       backendKey(cfg.keyPrefix, source.Key),
		VersionID: source.VersionID,
	}
	destinationKey := backendKey(cfg.keyPrefix, destination.Key)
	copyOut, err := cfg.client.CopyObject(ctx, &s3sdk.CopyObjectInput{
		Bucket:                aws.String(cfg.bucket),
		Key:                   aws.String(destinationKey),
		CopySource:            aws.String(copySourceHeader(cfg.bucket, sourceBackendRef)),
		CopySourceIfMatch:     awsStringIfSet(req.IfMatch),
		CopySourceIfNoneMatch: awsStringIfSet(req.IfNoneMatch),
	})
	if err != nil {
		return gestalt.ObjectMeta{}, toStatusError(fmt.Errorf("s3: copy object %s/%s to %s/%s: %w", cfg.bucket, sourceBackendRef.Key, cfg.bucket, destinationKey, err))
	}

	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Key:       destination.Key,
			VersionID: aws.ToString(copyOut.VersionId),
		},
	}
	if copyOut.CopyObjectResult != nil {
		meta.ETag = aws.ToString(copyOut.CopyObjectResult.ETag)
		if copyOut.CopyObjectResult.LastModified != nil {
			meta.LastModified = copyOut.CopyObjectResult.LastModified.UTC()
		}
	}
	if head, err := p.headObject(ctx, cfg, meta.Ref); err == nil {
		meta = head
	}
	return meta, nil
}

func (p *Provider) PresignObject(ctx context.Context, req gestalt.PresignRequest) (gestalt.PresignResult, error) {
	ref := req.Ref
	ref, err := validateObjectRef(ref, "ref")
	if err != nil {
		return gestalt.PresignResult{}, err
	}
	cfg, err := p.configured()
	if err != nil {
		return gestalt.PresignResult{}, toStatusError(err)
	}

	method := s3.PresignMethodGet
	var expires time.Duration
	var contentType, contentDisposition string
	var headers map[string]string
	method = req.Method
	expires = req.Expires
	contentType = req.ContentType
	contentDisposition = req.ContentDisposition
	headers = clonePresignHeaders(req.Headers)
	if method == "" {
		method = s3.PresignMethodGet
	}
	if expires < 0 {
		return gestalt.PresignResult{}, gestalt.InvalidArgument("expires must be >= 0")
	}

	key := backendKey(cfg.keyPrefix, ref.Key)
	var presigned *v4.PresignedHTTPRequest
	switch method {
	case s3.PresignMethodPut:
		input := &s3sdk.PutObjectInput{
			Bucket: aws.String(cfg.bucket),
			Key:    aws.String(key),
		}
		if contentType != "" {
			input.ContentType = aws.String(contentType)
		}
		if contentDisposition != "" {
			input.ContentDisposition = aws.String(contentDisposition)
			headers = setPresignHeader(headers, "Content-Disposition", contentDisposition)
		}
		if contentType != "" {
			headers = setPresignHeader(headers, "Content-Type", contentType)
		}
		presigned, err = cfg.presigner.PresignPutObject(ctx, input, presignOptions(expires, headers))
	case s3.PresignMethodDelete:
		presigned, err = cfg.presigner.PresignDeleteObject(ctx, &s3sdk.DeleteObjectInput{
			Bucket:    aws.String(cfg.bucket),
			Key:       aws.String(key),
			VersionId: awsStringIfPresent(ref.VersionID),
		}, presignOptions(expires, headers))
	case s3.PresignMethodHead:
		presigned, err = cfg.presigner.PresignHeadObject(ctx, &s3sdk.HeadObjectInput{
			Bucket:    aws.String(cfg.bucket),
			Key:       aws.String(key),
			VersionId: awsStringIfPresent(ref.VersionID),
		}, presignOptions(expires, headers))
	default:
		input := &s3sdk.GetObjectInput{
			Bucket:    aws.String(cfg.bucket),
			Key:       aws.String(key),
			VersionId: awsStringIfPresent(ref.VersionID),
		}
		if contentType != "" {
			input.ResponseContentType = aws.String(contentType)
		}
		if contentDisposition != "" {
			input.ResponseContentDisposition = aws.String(contentDisposition)
		}
		presigned, err = cfg.presigner.PresignGetObject(ctx, input, presignOptions(expires, headers))
	}
	if err != nil {
		return gestalt.PresignResult{}, toStatusError(fmt.Errorf("s3: presign object %s/%s: %w", cfg.bucket, key, err))
	}

	result := gestalt.PresignResult{
		URL:     presigned.URL,
		Method:  method,
		Headers: cloneStringMap(headers),
	}
	if expires > 0 {
		result.ExpiresAt = p.now().Add(expires)
	}
	return result, nil
}

func (p *Provider) configured() (configuredProvider, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.client == nil || p.presigner == nil {
		return configuredProvider{}, errNotConfigured
	}
	return configuredProvider{
		client:    p.client,
		presigner: p.presigner,
		bucket:    p.cfg.Bucket,
		keyPrefix: p.cfg.KeyPrefix,
	}, nil
}

func (p *Provider) headObject(ctx context.Context, cfg configuredProvider, ref gestalt.ObjectRef) (gestalt.ObjectMeta, error) {
	key := backendKey(cfg.keyPrefix, ref.Key)
	input := &s3sdk.HeadObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(key),
	}
	if ref.VersionID != "" {
		input.VersionId = aws.String(ref.VersionID)
	}
	out, err := cfg.client.HeadObject(ctx, input)
	if err != nil {
		return gestalt.ObjectMeta{}, fmt.Errorf("s3: head object %s/%s: %w", cfg.bucket, key, err)
	}
	return objectMetaFromHead(ref, out), nil
}

func (p *Provider) writeObject(ctx context.Context, cfg configuredProvider, ref gestalt.ObjectRef, opts *gestalt.WriteRequest, body io.Reader) (gestalt.ObjectMeta, error) {
	key := backendKey(cfg.keyPrefix, ref.Key)
	staged, size, err := stageBody(body)
	if err != nil {
		return gestalt.ObjectMeta{}, fmt.Errorf("s3: stage object %s/%s: %w", cfg.bucket, key, err)
	}
	defer func() {
		_ = staged.Close()
		_ = os.Remove(staged.Name())
	}()
	if size > maxSingleRequestObjectSize {
		return gestalt.ObjectMeta{}, fmt.Errorf("%w: object size %d exceeds single-request upload limit %d", errMultipartUploadRequired, size, maxSingleRequestObjectSize)
	}

	input := &s3sdk.PutObjectInput{
		Bucket:        aws.String(cfg.bucket),
		Key:           aws.String(key),
		Body:          staged,
		ContentLength: aws.Int64(size),
	}
	if opts != nil {
		if opts.ContentType != "" {
			input.ContentType = aws.String(opts.ContentType)
		}
		if opts.CacheControl != "" {
			input.CacheControl = aws.String(opts.CacheControl)
		}
		if opts.ContentDisposition != "" {
			input.ContentDisposition = aws.String(opts.ContentDisposition)
		}
		if opts.ContentEncoding != "" {
			input.ContentEncoding = aws.String(opts.ContentEncoding)
		}
		if opts.ContentLanguage != "" {
			input.ContentLanguage = aws.String(opts.ContentLanguage)
		}
		if opts.IfMatch != "" {
			input.IfMatch = aws.String(opts.IfMatch)
		}
		if opts.IfNoneMatch != "" {
			input.IfNoneMatch = aws.String(opts.IfNoneMatch)
		}
		if len(opts.Metadata) > 0 {
			input.Metadata = cloneStringMap(opts.Metadata)
		}
	}
	putOut, err := cfg.client.PutObject(ctx, input)
	if err != nil {
		return gestalt.ObjectMeta{}, fmt.Errorf("s3: put object %s/%s: %w", cfg.bucket, key, err)
	}
	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Key:       ref.Key,
			VersionID: aws.ToString(putOut.VersionId),
		},
		ETag: aws.ToString(putOut.ETag),
		Size: size,
	}
	if opts != nil {
		meta.ContentType = opts.ContentType
		meta.Metadata = cloneStringMap(opts.Metadata)
	}
	if now := p.now(); !now.IsZero() {
		meta.LastModified = now.UTC()
	}
	if head, err := p.headObject(ctx, cfg, meta.Ref); err == nil {
		meta = head
	}
	return meta, nil
}

func stageBody(body io.Reader) (*os.File, int64, error) {
	if body == nil {
		body = strings.NewReader("")
	}
	file, err := os.CreateTemp("", "gestalt-s3-upload-*")
	if err != nil {
		return nil, 0, err
	}
	size, err := io.Copy(file, body)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, 0, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, 0, err
	}
	return file, size, nil
}

func buildS3Client(ctx context.Context, cfg config) (*s3sdk.Client, *s3sdk.PresignClient, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, nil, err
	}

	client := s3sdk.NewFromConfig(awsCfg, s3ClientOptions(cfg, true))
	presignClient := client
	if cfg.PayloadSigning == payloadSigningSigned {
		presignClient = s3sdk.NewFromConfig(awsCfg, s3ClientOptions(cfg, false))
	}
	return client, s3sdk.NewPresignClient(presignClient), nil
}

func s3ClientOptions(cfg config, signPayloads bool) func(*s3sdk.Options) {
	return func(o *s3sdk.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		if cfg.ForcePathStyle {
			o.UsePathStyle = true
		}
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if signPayloads && cfg.PayloadSigning == payloadSigningSigned {
			o.APIOptions = append(o.APIOptions, forceSignedPayload)
		}
	}
}

func forceSignedPayload(stack *smithymiddleware.Stack) error {
	if _, err := stack.Finalize.Swap((&v4.ComputePayloadSHA256{}).ID(), &v4.ComputePayloadSHA256{}); err != nil {
		return err
	}
	// Keep transport/runtime headers out of SigV4 so S3-compatible backends only
	// verify stable object request headers. Accept-Encoding is restored unsigned.
	if err := stack.Finalize.Insert(trimSignedHeaders{}, signingMiddlewareID, smithymiddleware.Before); err != nil {
		return err
	}
	return stack.Finalize.Insert(restoreAcceptEncoding{}, signingMiddlewareID, smithymiddleware.After)
}

type trimSignedHeaders struct{}

func (trimSignedHeaders) ID() string { return "S3ProviderTrimSignedHeaders" }

func (trimSignedHeaders) HandleFinalize(ctx context.Context, in smithymiddleware.FinalizeInput, next smithymiddleware.FinalizeHandler) (smithymiddleware.FinalizeOutput, smithymiddleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if ok {
		req.Header.Del(acceptEncodingHeader)
		req.Header.Del(amzSDKInvocationID)
		req.Header.Del(amzSDKRequestHeader)
	}
	return next.HandleFinalize(ctx, in)
}

type restoreAcceptEncoding struct{}

func (restoreAcceptEncoding) ID() string { return "S3ProviderRestoreAcceptEncoding" }

func (restoreAcceptEncoding) HandleFinalize(ctx context.Context, in smithymiddleware.FinalizeInput, next smithymiddleware.FinalizeHandler) (smithymiddleware.FinalizeOutput, smithymiddleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if ok {
		req.Header.Set(acceptEncodingHeader, acceptEncodingIdentity)
	}
	return next.HandleFinalize(ctx, in)
}

func validateObjectRef(ref gestalt.ObjectRef, field string) (gestalt.ObjectRef, error) {
	key := ref.Key
	switch {
	case key == "":
		return gestalt.ObjectRef{}, gestalt.InvalidArgument(fmt.Sprintf("%s.key is required", field))
	default:
		return gestalt.ObjectRef{
			Key:       key,
			VersionID: ref.VersionID,
		}, nil
	}
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

func byteRangeHeader(r *gestalt.ByteRange) (string, error) {
	if r == nil {
		return "", nil
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
		return "", gestalt.OutOfRange("s3: invalid range")
	}
	if endSet && end < 0 {
		return "", gestalt.OutOfRange("s3: invalid range")
	}
	if startSet && endSet && start > end {
		return "", gestalt.OutOfRange("s3: invalid range")
	}
	switch {
	case startSet && endSet:
		return fmt.Sprintf("bytes=%d-%d", start, end), nil
	case startSet:
		return fmt.Sprintf("bytes=%d-", start), nil
	case endSet:
		return fmt.Sprintf("bytes=0-%d", end), nil
	default:
		return "", nil
	}
}

func awsStringIfSet(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return aws.String(value)
}

func awsStringIfPresent(value string) *string {
	if value == "" {
		return nil
	}
	return aws.String(value)
}

func objectMetaFromHead(ref gestalt.ObjectRef, out *s3sdk.HeadObjectOutput) gestalt.ObjectMeta {
	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Key:       ref.Key,
			VersionID: ref.VersionID,
		},
		ETag:         aws.ToString(out.ETag),
		Size:         aws.ToInt64(out.ContentLength),
		ContentType:  aws.ToString(out.ContentType),
		Metadata:     cloneStringMap(out.Metadata),
		StorageClass: string(out.StorageClass),
	}
	if out.VersionId != nil {
		meta.Ref.VersionID = aws.ToString(out.VersionId)
	}
	if out.LastModified != nil {
		meta.LastModified = out.LastModified.UTC()
	}
	return meta
}

func objectMetaFromGet(ref gestalt.ObjectRef, out *s3sdk.GetObjectOutput) gestalt.ObjectMeta {
	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Key:       ref.Key,
			VersionID: ref.VersionID,
		},
		ETag:         aws.ToString(out.ETag),
		Size:         aws.ToInt64(out.ContentLength),
		ContentType:  aws.ToString(out.ContentType),
		Metadata:     cloneStringMap(out.Metadata),
		StorageClass: string(out.StorageClass),
	}
	if total, ok := totalSizeFromContentRange(aws.ToString(out.ContentRange)); ok {
		meta.Size = total
	}
	if out.VersionId != nil {
		meta.Ref.VersionID = aws.ToString(out.VersionId)
	}
	if out.LastModified != nil {
		meta.LastModified = out.LastModified.UTC()
	}
	return meta
}

func objectMetaFromList(keyPrefix string, object s3types.Object) (gestalt.ObjectMeta, bool) {
	key, ok := logicalKey(keyPrefix, aws.ToString(object.Key))
	if !ok {
		return gestalt.ObjectMeta{}, false
	}
	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Key: key,
		},
		ETag:         aws.ToString(object.ETag),
		Size:         aws.ToInt64(object.Size),
		StorageClass: string(object.StorageClass),
	}
	if object.LastModified != nil {
		meta.LastModified = object.LastModified.UTC()
	}
	return meta, true
}

func copySourceHeader(bucket string, ref gestalt.ObjectRef) string {
	copySource := url.PathEscape(bucket) + "/" + escapeS3Path(ref.Key)
	if ref.VersionID == "" {
		return copySource
	}
	return copySource + "?versionId=" + url.QueryEscape(ref.VersionID)
}

func escapeS3Path(value string) string {
	parts := strings.Split(value, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func totalSizeFromContentRange(value string) (int64, bool) {
	if value == "" {
		return 0, false
	}
	slash := strings.LastIndexByte(value, '/')
	if slash < 0 || slash == len(value)-1 {
		return 0, false
	}
	totalText := value[slash+1:]
	if totalText == "*" {
		return 0, false
	}
	total, err := strconv.ParseInt(totalText, 10, 64)
	if err != nil {
		return 0, false
	}
	return total, true
}

func presignOptions(expires time.Duration, headers map[string]string) func(*s3sdk.PresignOptions) {
	return func(opts *s3sdk.PresignOptions) {
		if expires > 0 {
			opts.Expires = expires
		}
		if len(headers) == 0 {
			return
		}
		opts.ClientOptions = append(opts.ClientOptions, func(o *s3sdk.Options) {
			o.APIOptions = append(o.APIOptions, addPresignHeaders(headers))
		})
	}
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

func setPresignHeader(headers map[string]string, key, value string) map[string]string {
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

func addPresignHeaders(headers map[string]string) func(*smithymiddleware.Stack) error {
	return func(stack *smithymiddleware.Stack) error {
		return stack.Build.Add(smithymiddleware.BuildMiddlewareFunc(
			"GestaltPresignHeaders",
			func(ctx context.Context, in smithymiddleware.BuildInput, next smithymiddleware.BuildHandler) (
				out smithymiddleware.BuildOutput,
				metadata smithymiddleware.Metadata,
				err error,
			) {
				req, ok := in.Request.(*smithyhttp.Request)
				if ok {
					for key, value := range headers {
						req.Header.Set(key, value)
					}
				}
				return next.HandleBuild(ctx, in)
			},
		), smithymiddleware.After)
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

func toStatusError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return gestalt.Canceled(err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return gestalt.Canceled(err.Error())
	case errors.Is(err, errMultipartUploadRequired), errors.Is(err, errMultipartCopyRequired):
		return gestalt.Unimplemented(err.Error())
	case errors.Is(err, errNotConfigured):
		return gestalt.FailedPrecondition(err.Error())
	case errors.Is(err, s3.ErrNotFound), isNotFound(err):
		return gestalt.NotFound(err.Error())
	case isNotModified(err):
		return gestalt.FailedPrecondition(err.Error())
	case errors.Is(err, s3.ErrPreconditionFailed), isPreconditionFailed(err):
		return gestalt.FailedPrecondition(err.Error())
	case errors.Is(err, s3.ErrInvalidRange), isInvalidRange(err):
		return gestalt.OutOfRange(err.Error())
	case isPermissionDenied(err):
		return gestalt.PermissionDenied(err.Error())
	default:
		return gestalt.Internal(err.Error())
	}
}

func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket", "NoSuchVersion":
			return true
		}
	}
	return false
}

func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed":
			return true
		}
	}
	return false
}

func isInvalidRange(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "InvalidRange", "RequestedRangeNotSatisfiable":
			return true
		}
	}
	var respErr *smithyhttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusRequestedRangeNotSatisfiable
}

func isNotModified(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotModified" {
		return true
	}
	var respErr *smithyhttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == 304
}

func isAccessDenied(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDenied"
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

var _ gestalt.S3Provider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)

// Keep the import alive while the AWS SDK S3 XML enums are string-backed.
var _ s3types.ObjectStorageClass
