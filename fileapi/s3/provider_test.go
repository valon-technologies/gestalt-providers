package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConfigureValidatesAndNormalizes(t *testing.T) {
	t.Parallel()

	var got config
	provider := New()
	provider.newClient = func(_ context.Context, cfg config) (s3Client, error) {
		got = cfg
		return newFakeClient(), nil
	}

	err := provider.Configure(context.Background(), "primary", map[string]any{
		"bucket":         "  bucket-name  ",
		"region":         "us-east-1",
		"prefix":         "/nested/path/",
		"endpoint":       " http://localhost:4566 ",
		"forcePathStyle": true,
	})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}

	want := config{
		Bucket:         "bucket-name",
		Region:         "us-east-1",
		Prefix:         "nested/path",
		Endpoint:       "http://localhost:4566",
		ForcePathStyle: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Configure() config = %#v, want %#v", got, want)
	}
}

func TestConfigureRequiresBucketAndRegion(t *testing.T) {
	t.Parallel()

	provider := New()
	if err := provider.Configure(context.Background(), "s3", map[string]any{"region": "us-east-1"}); err == nil || !strings.Contains(err.Error(), "bucket is required") {
		t.Fatalf("Configure() missing bucket error = %v", err)
	}
	if err := provider.Configure(context.Background(), "s3", map[string]any{"bucket": "bucket"}); err == nil || !strings.Contains(err.Error(), "region is required") {
		t.Fatalf("Configure() missing region error = %v", err)
	}
}

func TestCreateFileStatReadSliceAndURLs(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	provider := configuredProvider(t, client, config{
		Bucket: "bucket",
		Region: "us-east-1",
		Prefix: "tenant-a",
	})
	ctx := context.Background()

	blob, err := provider.CreateBlob(ctx, &proto.CreateBlobRequest{
		Parts: []*proto.BlobPart{
			{Kind: &proto.BlobPart_StringData{StringData: "hello"}},
			{Kind: &proto.BlobPart_BytesData{BytesData: []byte(" world")}},
		},
		Options: &proto.BlobOptions{MimeType: "Text/Plain"},
	})
	if err != nil {
		t.Fatalf("CreateBlob() error = %v", err)
	}
	if blob.GetObject().GetKind() != proto.FileObjectKind_FILE_OBJECT_KIND_BLOB {
		t.Fatalf("CreateBlob() kind = %v", blob.GetObject().GetKind())
	}
	if blob.GetObject().GetType() != "text/plain" {
		t.Fatalf("CreateBlob() type = %q", blob.GetObject().GetType())
	}

	file, err := provider.CreateFile(ctx, &proto.CreateFileRequest{
		FileBits: []*proto.BlobPart{
			{Kind: &proto.BlobPart_BlobId{BlobId: blob.GetObject().GetId()}},
			{Kind: &proto.BlobPart_StringData{StringData: "\nnext"}},
		},
		FileName: "greeting.txt",
		Options: &proto.FileOptions{
			MimeType:     "TEXT/PLAIN",
			Endings:      proto.LineEndings_LINE_ENDINGS_TRANSPARENT,
			LastModified: 1234,
		},
	})
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if file.GetObject().GetKind() != proto.FileObjectKind_FILE_OBJECT_KIND_FILE {
		t.Fatalf("CreateFile() kind = %v", file.GetObject().GetKind())
	}
	if file.GetObject().GetName() != "greeting.txt" {
		t.Fatalf("CreateFile() name = %q", file.GetObject().GetName())
	}
	if file.GetObject().GetLastModified() != 1234 {
		t.Fatalf("CreateFile() lastModified = %d", file.GetObject().GetLastModified())
	}

	stat, err := provider.Stat(ctx, &proto.FileObjectRequest{Id: file.GetObject().GetId()})
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if stat.GetObject().GetType() != "text/plain" {
		t.Fatalf("Stat() type = %q", stat.GetObject().GetType())
	}
	if stat.GetObject().GetSize() != int64(len("hello world\nnext")) {
		t.Fatalf("Stat() size = %d", stat.GetObject().GetSize())
	}

	read, err := provider.ReadBytes(ctx, &proto.FileObjectRequest{Id: file.GetObject().GetId()})
	if err != nil {
		t.Fatalf("ReadBytes() error = %v", err)
	}
	if string(read.GetData()) != "hello world\nnext" {
		t.Fatalf("ReadBytes() data = %q", string(read.GetData()))
	}

	start, end := int64(6), int64(11)
	slice, err := provider.Slice(ctx, &proto.SliceRequest{
		Id:          file.GetObject().GetId(),
		Start:       &start,
		End:         &end,
		ContentType: "Text/Plain",
	})
	if err != nil {
		t.Fatalf("Slice() error = %v", err)
	}
	if slice.GetObject().GetKind() != proto.FileObjectKind_FILE_OBJECT_KIND_BLOB {
		t.Fatalf("Slice() kind = %v", slice.GetObject().GetKind())
	}
	sliceBytes, err := provider.ReadBytes(ctx, &proto.FileObjectRequest{Id: slice.GetObject().GetId()})
	if err != nil {
		t.Fatalf("ReadBytes(slice) error = %v", err)
	}
	if string(sliceBytes.GetData()) != "world" {
		t.Fatalf("ReadBytes(slice) data = %q", string(sliceBytes.GetData()))
	}

	urlResp, err := provider.CreateObjectURL(ctx, &proto.CreateObjectURLRequest{Id: file.GetObject().GetId()})
	if err != nil {
		t.Fatalf("CreateObjectURL() error = %v", err)
	}
	resolved, err := provider.ResolveObjectURL(ctx, &proto.ObjectURLRequest{Url: urlResp.GetUrl()})
	if err != nil {
		t.Fatalf("ResolveObjectURL() error = %v", err)
	}
	if resolved.GetObject().GetId() != file.GetObject().GetId() {
		t.Fatalf("ResolveObjectURL() id = %q, want %q", resolved.GetObject().GetId(), file.GetObject().GetId())
	}
	if _, err := provider.RevokeObjectURL(ctx, &proto.ObjectURLRequest{Url: urlResp.GetUrl()}); err != nil {
		t.Fatalf("RevokeObjectURL() error = %v", err)
	}
	if _, err := provider.ResolveObjectURL(ctx, &proto.ObjectURLRequest{Url: urlResp.GetUrl()}); status.Code(err) != codes.NotFound {
		t.Fatalf("ResolveObjectURL() after revoke code = %v, want %v", status.Code(err), codes.NotFound)
	}
}

func TestOpenReadStreamChunksData(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	provider := configuredProvider(t, client, config{Bucket: "bucket", Region: "us-east-1"})
	ctx := context.Background()

	payload := strings.Repeat("abcdefghij", 7000)
	blob, err := provider.CreateBlob(ctx, &proto.CreateBlobRequest{
		Parts:   []*proto.BlobPart{{Kind: &proto.BlobPart_StringData{StringData: payload}}},
		Options: &proto.BlobOptions{MimeType: "text/plain"},
	})
	if err != nil {
		t.Fatalf("CreateBlob() error = %v", err)
	}

	stream := &fakeReadStream{ctx: ctx}
	if err := provider.OpenReadStream(&proto.ReadStreamRequest{Id: blob.GetObject().GetId()}, stream); err != nil {
		t.Fatalf("OpenReadStream() error = %v", err)
	}
	if len(stream.chunks) < 2 {
		t.Fatalf("OpenReadStream() chunks = %d, want >= 2", len(stream.chunks))
	}
	if got := string(bytes.Join(stream.chunks, nil)); got != payload {
		t.Fatalf("OpenReadStream() payload mismatch")
	}
}

func TestNotFoundMapsToGRPCNotFound(t *testing.T) {
	t.Parallel()

	provider := configuredProvider(t, newFakeClient(), config{Bucket: "bucket", Region: "us-east-1"})
	_, err := provider.Stat(context.Background(), &proto.FileObjectRequest{Id: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Stat() code = %v, want %v", status.Code(err), codes.NotFound)
	}
}

func TestHealthCheckUsesHeadBucket(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	provider := configuredProvider(t, client, config{Bucket: "bucket", Region: "us-east-1"})
	if err := provider.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck() error = %v", err)
	}
	if client.headBucketCalls != 1 {
		t.Fatalf("HeadBucket() calls = %d, want 1", client.headBucketCalls)
	}
}

type fakeClient struct {
	headBucketCalls int
	objects         map[string]fakeObject
}

type fakeObject struct {
	body        []byte
	contentType string
	metadata    map[string]string
}

func newFakeClient() *fakeClient {
	return &fakeClient{objects: make(map[string]fakeObject)}
}

func (f *fakeClient) HeadBucket(context.Context, *s3sdk.HeadBucketInput, ...func(*s3sdk.Options)) (*s3sdk.HeadBucketOutput, error) {
	f.headBucketCalls++
	return &s3sdk.HeadBucketOutput{}, nil
}

func (f *fakeClient) PutObject(_ context.Context, in *s3sdk.PutObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.PutObjectOutput, error) {
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	metadata := make(map[string]string, len(in.Metadata))
	for k, v := range in.Metadata {
		metadata[k] = v
	}
	f.objects[aws.ToString(in.Key)] = fakeObject{
		body:        data,
		contentType: aws.ToString(in.ContentType),
		metadata:    metadata,
	}
	return &s3sdk.PutObjectOutput{}, nil
}

func (f *fakeClient) HeadObject(_ context.Context, in *s3sdk.HeadObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.HeadObjectOutput, error) {
	object, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "missing"}
	}
	metadata := make(map[string]string, len(object.metadata))
	for k, v := range object.metadata {
		metadata[k] = v
	}
	contentLength := int64(len(object.body))
	return &s3sdk.HeadObjectOutput{
		ContentLength: &contentLength,
		ContentType:   aws.String(object.contentType),
		Metadata:      metadata,
	}, nil
}

func (f *fakeClient) GetObject(_ context.Context, in *s3sdk.GetObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.GetObjectOutput, error) {
	object, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "missing"}
	}
	return &s3sdk.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(object.body))}, nil
}

type fakeReadStream struct {
	proto.FileAPI_OpenReadStreamServer
	ctx    context.Context
	chunks [][]byte
}

func (f *fakeReadStream) Context() context.Context { return f.ctx }

func (f *fakeReadStream) Send(chunk *proto.ReadChunk) error {
	if chunk == nil {
		return errors.New("nil chunk")
	}
	f.chunks = append(f.chunks, append([]byte(nil), chunk.GetData()...))
	return nil
}

func configuredProvider(t *testing.T, client s3Client, cfg config) *Provider {
	t.Helper()
	provider := New()
	provider.newClient = func(context.Context, config) (s3Client, error) { return client, nil }
	if err := provider.Configure(context.Background(), "s3", map[string]any{
		"bucket":         cfg.Bucket,
		"region":         cfg.Region,
		"prefix":         cfg.Prefix,
		"endpoint":       cfg.Endpoint,
		"forcePathStyle": cfg.ForcePathStyle,
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	return provider
}
