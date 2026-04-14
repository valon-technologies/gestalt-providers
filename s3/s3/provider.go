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
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion = "0.0.1-alpha.1"
	streamChunkSize = 64 * 1024
)

var (
	errNotConfigured                 = errors.New("s3: not configured")
	errMultipartUploadRequired       = errors.New("s3: multipart upload required")
	errMultipartCopyRequired         = errors.New("s3: multipart copy required")
	maxSingleRequestObjectSize int64 = 5 * 1024 * 1024 * 1024
)

type config struct {
	Region          string `yaml:"region"`
	Endpoint        string `yaml:"endpoint"`
	ForcePathStyle  bool   `yaml:"forcePathStyle"`
	AccessKeyID     string `yaml:"accessKeyId"`
	SecretAccessKey string `yaml:"secretAccessKey"`
	SessionToken    string `yaml:"sessionToken"`
}

type Provider struct {
	proto.UnimplementedS3Server

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
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.AccessKeyID = strings.TrimSpace(cfg.AccessKeyID)
	cfg.SecretAccessKey = strings.TrimSpace(cfg.SecretAccessKey)
	cfg.SessionToken = strings.TrimSpace(cfg.SessionToken)

	switch {
	case cfg.Region == "":
		return fmt.Errorf("s3: region is required")
	case (cfg.AccessKeyID == "") != (cfg.SecretAccessKey == ""):
		return fmt.Errorf("s3: accessKeyId and secretAccessKey must be provided together")
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
	client, _, err := p.configured()
	if err != nil {
		return err
	}
	healthCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = client.ListBuckets(healthCtx, &s3sdk.ListBucketsInput{})
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

func (p *Provider) HeadObject(ctx context.Context, req *proto.HeadObjectRequest) (*proto.HeadObjectResponse, error) {
	ref, err := validateObjectRef(req.GetRef(), "ref")
	if err != nil {
		return nil, err
	}
	meta, err := p.headObject(ctx, ref)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &proto.HeadObjectResponse{Meta: objectMetaToProto(meta)}, nil
}

func (p *Provider) ReadObject(req *proto.ReadObjectRequest, stream proto.S3_ReadObjectServer) error {
	ref, err := validateObjectRef(req.GetRef(), "ref")
	if err != nil {
		return err
	}

	client, _, err := p.configured()
	if err != nil {
		return toStatusError(err)
	}

	input := &s3sdk.GetObjectInput{
		Bucket: aws.String(ref.Bucket),
		Key:    aws.String(ref.Key),
	}
	if ref.VersionID != "" {
		input.VersionId = aws.String(ref.VersionID)
	}
	if req.GetIfMatch() != "" {
		input.IfMatch = aws.String(req.GetIfMatch())
	}
	if req.GetIfNoneMatch() != "" {
		input.IfNoneMatch = aws.String(req.GetIfNoneMatch())
	}
	if ts := req.GetIfModifiedSince(); ts != nil {
		t := ts.AsTime()
		input.IfModifiedSince = &t
	}
	if ts := req.GetIfUnmodifiedSince(); ts != nil {
		t := ts.AsTime()
		input.IfUnmodifiedSince = &t
	}
	if req.GetRange() != nil {
		rangeHeader, err := byteRangeHeader(req.GetRange())
		if err != nil {
			return err
		}
		if rangeHeader != "" {
			input.Range = aws.String(rangeHeader)
		}
	}

	out, err := client.GetObject(stream.Context(), input)
	if err != nil {
		return toStatusError(fmt.Errorf("s3: get object %s/%s: %w", ref.Bucket, ref.Key, err))
	}
	defer out.Body.Close()

	meta := objectMetaFromGet(ref, out)

	if err := stream.Send(&proto.ReadObjectChunk{
		Result: &proto.ReadObjectChunk_Meta{Meta: objectMetaToProto(meta)},
	}); err != nil {
		return err
	}

	buf := make([]byte, streamChunkSize)
	for {
		n, readErr := out.Body.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			if err := stream.Send(&proto.ReadObjectChunk{
				Result: &proto.ReadObjectChunk_Data{Data: chunk},
			}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return toStatusError(fmt.Errorf("s3: read object %s/%s: %w", ref.Bucket, ref.Key, readErr))
		}
	}
}

func (p *Provider) WriteObject(stream proto.S3_WriteObjectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "first message must be WriteObjectOpen")
	}
	ref, err := validateObjectRef(open.GetRef(), "open.ref")
	if err != nil {
		return err
	}

	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer func() { _ = pw.Close() }()
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				done <- nil
				return
			}
			if err != nil {
				_ = pw.CloseWithError(err)
				done <- err
				return
			}
			data := msg.GetData()
			if len(data) == 0 {
				continue
			}
			if _, err := pw.Write(data); err != nil {
				done <- err
				return
			}
		}
	}()

	meta, err := p.writeObject(stream.Context(), ref, open, pr)
	_ = pr.Close()
	if err != nil {
		_ = pw.CloseWithError(err)
		return toStatusError(err)
	}
	recvErr := <-done
	if recvErr != nil && !isBenignWriteCompletionError(recvErr) {
		return recvErr
	}
	if err := stream.SendAndClose(&proto.WriteObjectResponse{Meta: objectMetaToProto(meta)}); err != nil {
		if isBenignWriteCompletionError(err) {
			return nil
		}
		return err
	}
	return nil
}

func (p *Provider) DeleteObject(ctx context.Context, req *proto.DeleteObjectRequest) (*emptypb.Empty, error) {
	ref, err := validateObjectRef(req.GetRef(), "ref")
	if err != nil {
		return nil, err
	}
	client, _, err := p.configured()
	if err != nil {
		return nil, toStatusError(err)
	}
	input := &s3sdk.DeleteObjectInput{
		Bucket: aws.String(ref.Bucket),
		Key:    aws.String(ref.Key),
	}
	if ref.VersionID != "" {
		input.VersionId = aws.String(ref.VersionID)
	}
	if _, err := client.DeleteObject(ctx, input); err != nil {
		return nil, toStatusError(fmt.Errorf("s3: delete object %s/%s: %w", ref.Bucket, ref.Key, err))
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) ListObjects(ctx context.Context, req *proto.ListObjectsRequest) (*proto.ListObjectsResponse, error) {
	bucket := strings.TrimSpace(req.GetBucket())
	if bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}
	client, _, err := p.configured()
	if err != nil {
		return nil, toStatusError(err)
	}
	input := &s3sdk.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}
	if req.GetPrefix() != "" {
		input.Prefix = aws.String(req.GetPrefix())
	}
	if req.GetDelimiter() != "" {
		input.Delimiter = aws.String(req.GetDelimiter())
	}
	if req.GetContinuationToken() != "" {
		input.ContinuationToken = aws.String(req.GetContinuationToken())
	}
	if req.GetStartAfter() != "" {
		input.StartAfter = aws.String(req.GetStartAfter())
	}
	if req.GetMaxKeys() > 0 {
		input.MaxKeys = aws.Int32(req.GetMaxKeys())
	}

	out, err := client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, toStatusError(fmt.Errorf("s3: list objects in %s: %w", bucket, err))
	}

	resp := &proto.ListObjectsResponse{
		CommonPrefixes:        make([]string, 0, len(out.CommonPrefixes)),
		NextContinuationToken: aws.ToString(out.NextContinuationToken),
		HasMore:               aws.ToBool(out.IsTruncated),
		Objects:               make([]*proto.S3ObjectMeta, 0, len(out.Contents)),
	}
	for _, prefix := range out.CommonPrefixes {
		resp.CommonPrefixes = append(resp.CommonPrefixes, aws.ToString(prefix.Prefix))
	}
	for _, object := range out.Contents {
		resp.Objects = append(resp.Objects, objectMetaToProto(objectMetaFromList(bucket, object)))
	}
	return resp, nil
}

func (p *Provider) CopyObject(ctx context.Context, req *proto.CopyObjectRequest) (*proto.CopyObjectResponse, error) {
	source, err := validateObjectRef(req.GetSource(), "source")
	if err != nil {
		return nil, err
	}
	destination, err := validateObjectRef(req.GetDestination(), "destination")
	if err != nil {
		return nil, err
	}

	client, _, err := p.configured()
	if err != nil {
		return nil, toStatusError(err)
	}
	sourceHead, err := p.headObject(ctx, source)
	if err != nil {
		return nil, toStatusError(err)
	}
	if sourceHead.Size > maxSingleRequestObjectSize {
		return nil, toStatusError(fmt.Errorf("%w: object size %d exceeds single-request copy limit %d", errMultipartCopyRequired, sourceHead.Size, maxSingleRequestObjectSize))
	}

	copyOut, err := client.CopyObject(ctx, &s3sdk.CopyObjectInput{
		Bucket:                aws.String(destination.Bucket),
		Key:                   aws.String(destination.Key),
		CopySource:            aws.String(copySourceHeader(source)),
		CopySourceIfMatch:     awsStringIfSet(req.GetIfMatch()),
		CopySourceIfNoneMatch: awsStringIfSet(req.GetIfNoneMatch()),
	})
	if err != nil {
		return nil, toStatusError(fmt.Errorf("s3: copy object %s/%s to %s/%s: %w", source.Bucket, source.Key, destination.Bucket, destination.Key, err))
	}

	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Bucket:    destination.Bucket,
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
	if head, err := p.headObject(ctx, meta.Ref); err == nil {
		meta = head
	}
	return &proto.CopyObjectResponse{Meta: objectMetaToProto(meta)}, nil
}

func (p *Provider) PresignObject(ctx context.Context, req *proto.PresignObjectRequest) (*proto.PresignObjectResponse, error) {
	ref, err := validateObjectRef(req.GetRef(), "ref")
	if err != nil {
		return nil, err
	}
	_, presigner, err := p.configured()
	if err != nil {
		return nil, toStatusError(err)
	}

	method := presignMethodFromProto(req.GetMethod())
	if method == "" {
		method = gestalt.PresignMethodGet
	}
	if req.GetExpiresSeconds() < 0 {
		return nil, status.Error(codes.InvalidArgument, "expiresSeconds must be >= 0")
	}
	headers := clonePresignHeaders(req.GetHeaders())
	expires := time.Duration(req.GetExpiresSeconds()) * time.Second

	var presigned *v4.PresignedHTTPRequest
	switch method {
	case gestalt.PresignMethodPut:
		input := &s3sdk.PutObjectInput{
			Bucket: aws.String(ref.Bucket),
			Key:    aws.String(ref.Key),
		}
		if req.GetContentType() != "" {
			input.ContentType = aws.String(req.GetContentType())
		}
		if req.GetContentDisposition() != "" {
			input.ContentDisposition = aws.String(req.GetContentDisposition())
			headers = setPresignHeader(headers, "Content-Disposition", req.GetContentDisposition())
		}
		if req.GetContentType() != "" {
			headers = setPresignHeader(headers, "Content-Type", req.GetContentType())
		}
		presigned, err = presigner.PresignPutObject(ctx, input, presignOptions(expires, headers))
	case gestalt.PresignMethodDelete:
		presigned, err = presigner.PresignDeleteObject(ctx, &s3sdk.DeleteObjectInput{
			Bucket:    aws.String(ref.Bucket),
			Key:       aws.String(ref.Key),
			VersionId: awsStringIfPresent(ref.VersionID),
		}, presignOptions(expires, headers))
	case gestalt.PresignMethodHead:
		presigned, err = presigner.PresignHeadObject(ctx, &s3sdk.HeadObjectInput{
			Bucket:    aws.String(ref.Bucket),
			Key:       aws.String(ref.Key),
			VersionId: awsStringIfPresent(ref.VersionID),
		}, presignOptions(expires, headers))
	default:
		input := &s3sdk.GetObjectInput{
			Bucket:    aws.String(ref.Bucket),
			Key:       aws.String(ref.Key),
			VersionId: awsStringIfPresent(ref.VersionID),
		}
		if req.GetContentType() != "" {
			input.ResponseContentType = aws.String(req.GetContentType())
		}
		if req.GetContentDisposition() != "" {
			input.ResponseContentDisposition = aws.String(req.GetContentDisposition())
		}
		presigned, err = presigner.PresignGetObject(ctx, input, presignOptions(expires, headers))
	}
	if err != nil {
		return nil, toStatusError(fmt.Errorf("s3: presign object %s/%s: %w", ref.Bucket, ref.Key, err))
	}

	resp := &proto.PresignObjectResponse{
		Url:     presigned.URL,
		Method:  presignMethodToProto(method),
		Headers: cloneStringMap(headers),
	}
	if expires > 0 {
		resp.ExpiresAt = timestamppb.New(p.now().Add(expires))
	}
	return resp, nil
}

func (p *Provider) configured() (*s3sdk.Client, *s3sdk.PresignClient, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.client == nil || p.presigner == nil {
		return nil, nil, errNotConfigured
	}
	return p.client, p.presigner, nil
}

func (p *Provider) headObject(ctx context.Context, ref gestalt.ObjectRef) (gestalt.ObjectMeta, error) {
	client, _, err := p.configured()
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	input := &s3sdk.HeadObjectInput{
		Bucket: aws.String(ref.Bucket),
		Key:    aws.String(ref.Key),
	}
	if ref.VersionID != "" {
		input.VersionId = aws.String(ref.VersionID)
	}
	out, err := client.HeadObject(ctx, input)
	if err != nil {
		return gestalt.ObjectMeta{}, fmt.Errorf("s3: head object %s/%s: %w", ref.Bucket, ref.Key, err)
	}
	return objectMetaFromHead(ref, out), nil
}

func (p *Provider) writeObject(ctx context.Context, ref gestalt.ObjectRef, open *proto.WriteObjectOpen, body io.Reader) (gestalt.ObjectMeta, error) {
	client, _, err := p.configured()
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}

	staged, size, err := stageBody(body)
	if err != nil {
		return gestalt.ObjectMeta{}, fmt.Errorf("s3: stage object %s/%s: %w", ref.Bucket, ref.Key, err)
	}
	defer func() {
		_ = staged.Close()
		_ = os.Remove(staged.Name())
	}()
	if size > maxSingleRequestObjectSize {
		return gestalt.ObjectMeta{}, fmt.Errorf("%w: object size %d exceeds single-request upload limit %d", errMultipartUploadRequired, size, maxSingleRequestObjectSize)
	}

	input := &s3sdk.PutObjectInput{
		Bucket:        aws.String(ref.Bucket),
		Key:           aws.String(ref.Key),
		Body:          staged,
		ContentLength: aws.Int64(size),
	}
	if open.GetContentType() != "" {
		input.ContentType = aws.String(open.GetContentType())
	}
	if open.GetCacheControl() != "" {
		input.CacheControl = aws.String(open.GetCacheControl())
	}
	if open.GetContentDisposition() != "" {
		input.ContentDisposition = aws.String(open.GetContentDisposition())
	}
	if open.GetContentEncoding() != "" {
		input.ContentEncoding = aws.String(open.GetContentEncoding())
	}
	if open.GetContentLanguage() != "" {
		input.ContentLanguage = aws.String(open.GetContentLanguage())
	}
	if open.GetIfMatch() != "" {
		input.IfMatch = aws.String(open.GetIfMatch())
	}
	if open.GetIfNoneMatch() != "" {
		input.IfNoneMatch = aws.String(open.GetIfNoneMatch())
	}
	if len(open.GetMetadata()) > 0 {
		input.Metadata = cloneStringMap(open.GetMetadata())
	}
	putOut, err := client.PutObject(ctx, input)
	if err != nil {
		return gestalt.ObjectMeta{}, fmt.Errorf("s3: put object %s/%s: %w", ref.Bucket, ref.Key, err)
	}
	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Bucket:    ref.Bucket,
			Key:       ref.Key,
			VersionID: aws.ToString(putOut.VersionId),
		},
		ETag:        aws.ToString(putOut.ETag),
		Size:        size,
		ContentType: open.GetContentType(),
		Metadata:    cloneStringMap(open.GetMetadata()),
	}
	if now := p.now(); !now.IsZero() {
		meta.LastModified = now.UTC()
	}
	if head, err := p.headObject(ctx, meta.Ref); err == nil {
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

	client := s3sdk.NewFromConfig(awsCfg, func(o *s3sdk.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		if cfg.ForcePathStyle {
			o.UsePathStyle = true
		}
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return client, s3sdk.NewPresignClient(client), nil
}

func validateObjectRef(ref *proto.S3ObjectRef, field string) (gestalt.ObjectRef, error) {
	if ref == nil {
		return gestalt.ObjectRef{}, status.Errorf(codes.InvalidArgument, "%s is required", field)
	}
	bucket := strings.TrimSpace(ref.GetBucket())
	key := ref.GetKey()
	switch {
	case bucket == "":
		return gestalt.ObjectRef{}, status.Errorf(codes.InvalidArgument, "%s.bucket is required", field)
	case key == "":
		return gestalt.ObjectRef{}, status.Errorf(codes.InvalidArgument, "%s.key is required", field)
	default:
		return gestalt.ObjectRef{
			Bucket:    bucket,
			Key:       key,
			VersionID: ref.GetVersionId(),
		}, nil
	}
}

func byteRangeHeader(r *proto.ByteRange) (string, error) {
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
		return "", status.Error(codes.OutOfRange, "s3: invalid range")
	}
	if endSet && end < 0 {
		return "", status.Error(codes.OutOfRange, "s3: invalid range")
	}
	if startSet && endSet && start > end {
		return "", status.Error(codes.OutOfRange, "s3: invalid range")
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
			Bucket:    ref.Bucket,
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
			Bucket:    ref.Bucket,
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

func objectMetaFromList(bucket string, object s3types.Object) gestalt.ObjectMeta {
	meta := gestalt.ObjectMeta{
		Ref: gestalt.ObjectRef{
			Bucket: bucket,
			Key:    aws.ToString(object.Key),
		},
		ETag:         aws.ToString(object.ETag),
		Size:         aws.ToInt64(object.Size),
		StorageClass: string(object.StorageClass),
	}
	if object.LastModified != nil {
		meta.LastModified = object.LastModified.UTC()
	}
	return meta
}

func copySourceHeader(ref gestalt.ObjectRef) string {
	copySource := url.PathEscape(ref.Bucket) + "/" + escapeS3Path(ref.Key)
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

func objectMetaToProto(meta gestalt.ObjectMeta) *proto.S3ObjectMeta {
	out := &proto.S3ObjectMeta{
		Ref: &proto.S3ObjectRef{
			Bucket:    meta.Ref.Bucket,
			Key:       meta.Ref.Key,
			VersionId: meta.Ref.VersionID,
		},
		Etag:         meta.ETag,
		Size:         meta.Size,
		ContentType:  meta.ContentType,
		Metadata:     cloneStringMap(meta.Metadata),
		StorageClass: meta.StorageClass,
	}
	if !meta.LastModified.IsZero() {
		out.LastModified = timestamppb.New(meta.LastModified)
	}
	return out
}

func presignMethodFromProto(method proto.PresignMethod) gestalt.PresignMethod {
	switch method {
	case proto.PresignMethod_PRESIGN_METHOD_GET:
		return gestalt.PresignMethodGet
	case proto.PresignMethod_PRESIGN_METHOD_PUT:
		return gestalt.PresignMethodPut
	case proto.PresignMethod_PRESIGN_METHOD_DELETE:
		return gestalt.PresignMethodDelete
	case proto.PresignMethod_PRESIGN_METHOD_HEAD:
		return gestalt.PresignMethodHead
	default:
		return ""
	}
}

func presignMethodToProto(method gestalt.PresignMethod) proto.PresignMethod {
	switch method {
	case gestalt.PresignMethodGet:
		return proto.PresignMethod_PRESIGN_METHOD_GET
	case gestalt.PresignMethodPut:
		return proto.PresignMethod_PRESIGN_METHOD_PUT
	case gestalt.PresignMethodDelete:
		return proto.PresignMethod_PRESIGN_METHOD_DELETE
	case gestalt.PresignMethodHead:
		return proto.PresignMethod_PRESIGN_METHOD_HEAD
	default:
		return proto.PresignMethod_PRESIGN_METHOD_UNSPECIFIED
	}
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
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, errMultipartUploadRequired), errors.Is(err, errMultipartCopyRequired):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, errNotConfigured):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, gestalt.ErrS3NotFound), isNotFound(err):
		return status.Error(codes.NotFound, err.Error())
	case isNotModified(err):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, gestalt.ErrS3PreconditionFailed), isPreconditionFailed(err):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, gestalt.ErrS3InvalidRange), isInvalidRange(err):
		return status.Error(codes.OutOfRange, err.Error())
	case isPermissionDenied(err):
		return status.Error(codes.PermissionDenied, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
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

func isBenignWriteCompletionError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.ErrClosedPipe)
}

var _ gestalt.S3Provider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)

// Keep the import alive while the AWS SDK S3 XML enums are string-backed.
var _ s3types.ObjectStorageClass
