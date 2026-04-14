package gcs

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeObject struct {
	data        []byte
	contentType string
	metadata    map[string]string
	updated     time.Time
}

type fakeStore struct {
	objects map[string]fakeObject
	now     func() time.Time
}

func newFakeStore(now func() time.Time) *fakeStore {
	return &fakeStore{
		objects: make(map[string]fakeObject),
		now:     now,
	}
}

func (s *fakeStore) HealthCheck(context.Context) error {
	return nil
}

func (s *fakeStore) Stat(_ context.Context, id string) (*objectAttrs, error) {
	object, ok := s.objects[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "file object not found")
	}
	return &objectAttrs{
		ID:          id,
		Size:        int64(len(object.data)),
		ContentType: object.contentType,
		Metadata:    cloneStringMap(object.metadata),
		Updated:     object.updated,
	}, nil
}

func (s *fakeStore) Write(_ context.Context, id string, data []byte, contentType string, metadata map[string]string) (*objectAttrs, error) {
	object := fakeObject{
		data:        bytes.Clone(data),
		contentType: contentType,
		metadata:    cloneStringMap(metadata),
		updated:     s.now(),
	}
	s.objects[id] = object
	return s.Stat(context.Background(), id)
}

func (s *fakeStore) ReadAll(_ context.Context, id string) ([]byte, error) {
	object, ok := s.objects[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "file object not found")
	}
	return bytes.Clone(object.data), nil
}

func (s *fakeStore) OpenRange(_ context.Context, id string, offset, length int64) (io.ReadCloser, error) {
	object, ok := s.objects[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "file object not found")
	}
	start := int(offset)
	if start > len(object.data) {
		start = len(object.data)
	}
	end := len(object.data)
	if length >= 0 && start+int(length) < end {
		end = start + int(length)
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(object.data[start:end]))), nil
}

func (s *fakeStore) Close() error {
	return nil
}

type captureReadStream struct {
	ctx    context.Context
	chunks [][]byte
}

func (s *captureReadStream) SetHeader(metadata.MD) error  { return nil }
func (s *captureReadStream) SendHeader(metadata.MD) error { return nil }
func (s *captureReadStream) SetTrailer(metadata.MD)       {}
func (s *captureReadStream) Context() context.Context     { return s.ctx }
func (s *captureReadStream) SendMsg(any) error            { return nil }
func (s *captureReadStream) RecvMsg(any) error            { return nil }

func (s *captureReadStream) Send(chunk *proto.ReadChunk) error {
	s.chunks = append(s.chunks, bytes.Clone(chunk.GetData()))
	return nil
}

func testProvider(t *testing.T) (*Provider, *fakeStore, time.Time) {
	t.Helper()
	now := time.UnixMilli(1_730_000_000_000)
	store := newFakeStore(func() time.Time { return now })
	provider := &Provider{
		name:   "gcs",
		prefix: "tenant/",
		store:  store,
		urls: newObjectURLStore(func() string {
			return "url-1"
		}),
		newID: func() string {
			return "obj-1"
		},
		now: func() time.Time {
			return now
		},
		timeout: time.Second,
	}
	return provider, store, now
}

func TestConfigureRequiresBucket(t *testing.T) {
	t.Parallel()

	provider := New()
	err := provider.Configure(context.Background(), "gcs", map[string]any{})
	if err == nil || err.Error() != "gcs fileapi: bucket is required" {
		t.Fatalf("Configure() error = %v, want bucket is required", err)
	}
}

func TestCreateBlobReadBytesAndStat(t *testing.T) {
	t.Parallel()

	provider, store, now := testProvider(t)
	store.objects["tenant/source"] = fakeObject{
		data:        []byte(" world"),
		contentType: "text/plain",
		metadata:    objectMetadata(proto.FileObjectKind_FILE_OBJECT_KIND_BLOB, "", now.UnixMilli()),
		updated:     now,
	}

	resp, err := provider.CreateBlob(context.Background(), &proto.CreateBlobRequest{
		Parts: []*proto.BlobPart{
			{Kind: &proto.BlobPart_StringData{StringData: "hello"}},
			{Kind: &proto.BlobPart_BlobId{BlobId: "tenant/source"}},
		},
		Options: &proto.BlobOptions{MimeType: "Text/Plain"},
	})
	if err != nil {
		t.Fatalf("CreateBlob() error = %v", err)
	}

	object := resp.GetObject()
	if object.GetId() != "tenant/obj-1" {
		t.Fatalf("CreateBlob() id = %q, want tenant/obj-1", object.GetId())
	}
	if object.GetKind() != proto.FileObjectKind_FILE_OBJECT_KIND_BLOB {
		t.Fatalf("CreateBlob() kind = %v, want blob", object.GetKind())
	}
	if object.GetType() != "text/plain" {
		t.Fatalf("CreateBlob() type = %q, want text/plain", object.GetType())
	}

	bytesResp, err := provider.ReadBytes(context.Background(), &proto.FileObjectRequest{Id: object.GetId()})
	if err != nil {
		t.Fatalf("ReadBytes() error = %v", err)
	}
	if got := string(bytesResp.GetData()); got != "hello world" {
		t.Fatalf("ReadBytes() = %q, want hello world", got)
	}

	statResp, err := provider.Stat(context.Background(), &proto.FileObjectRequest{Id: object.GetId()})
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if statResp.GetObject().GetLastModified() != 0 {
		t.Fatalf("Stat() last_modified = %d, want 0", statResp.GetObject().GetLastModified())
	}
}

func TestCreateFilePreservesZeroLastModifiedAndEmptyType(t *testing.T) {
	t.Parallel()

	provider, _, _ := testProvider(t)

	resp, err := provider.CreateFile(context.Background(), &proto.CreateFileRequest{
		FileBits: []*proto.BlobPart{
			{Kind: &proto.BlobPart_StringData{StringData: "notes"}},
		},
		FileName: "notes.txt",
		Options:  &proto.FileOptions{LastModified: 0},
	})
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}

	object := resp.GetObject()
	if object.GetKind() != proto.FileObjectKind_FILE_OBJECT_KIND_FILE {
		t.Fatalf("CreateFile() kind = %v, want file", object.GetKind())
	}
	if object.GetName() != "notes.txt" {
		t.Fatalf("CreateFile() name = %q, want notes.txt", object.GetName())
	}
	if object.GetLastModified() != 0 {
		t.Fatalf("CreateFile() last_modified = %d, want 0", object.GetLastModified())
	}
	if object.GetType() != "" {
		t.Fatalf("CreateFile() type = %q, want empty", object.GetType())
	}
}

func TestCreateFileNormalizesTypeAndResolvesNegativeLastModified(t *testing.T) {
	t.Parallel()

	provider, _, now := testProvider(t)

	resp, err := provider.CreateFile(context.Background(), &proto.CreateFileRequest{
		FileBits: []*proto.BlobPart{
			{Kind: &proto.BlobPart_StringData{StringData: "notes"}},
		},
		FileName: "notes.txt",
		Options: &proto.FileOptions{
			MimeType:     "Text/Plain",
			LastModified: -1,
		},
	})
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}

	object := resp.GetObject()
	if object.GetType() != "text/plain" {
		t.Fatalf("CreateFile() type = %q, want text/plain", object.GetType())
	}
	if object.GetLastModified() != now.UnixMilli() {
		t.Fatalf("CreateFile() last_modified = %d, want %d", object.GetLastModified(), now.UnixMilli())
	}
}

func TestSliceCreatesMaterializedBlob(t *testing.T) {
	t.Parallel()

	provider, store, now := testProvider(t)
	provider.newID = func() string { return "slice-1" }
	store.objects["tenant/original"] = fakeObject{
		data:        []byte("abcdef"),
		contentType: "text/plain",
		metadata:    objectMetadata(proto.FileObjectKind_FILE_OBJECT_KIND_FILE, "original.txt", now.UnixMilli()),
		updated:     now,
	}
	start := int64(-4)
	end := int64(-1)

	resp, err := provider.Slice(context.Background(), &proto.SliceRequest{
		Id:          "tenant/original",
		Start:       &start,
		End:         &end,
		ContentType: "Application/Custom",
	})
	if err != nil {
		t.Fatalf("Slice() error = %v", err)
	}

	object := resp.GetObject()
	if object.GetId() != "tenant/slice-1" {
		t.Fatalf("Slice() id = %q, want tenant/slice-1", object.GetId())
	}
	if object.GetKind() != proto.FileObjectKind_FILE_OBJECT_KIND_BLOB {
		t.Fatalf("Slice() kind = %v, want blob", object.GetKind())
	}
	if object.GetType() != "application/custom" {
		t.Fatalf("Slice() type = %q, want application/custom", object.GetType())
	}
	if object.GetLastModified() != 0 {
		t.Fatalf("Slice() last_modified = %d, want 0", object.GetLastModified())
	}

	bytesResp, err := provider.ReadBytes(context.Background(), &proto.FileObjectRequest{Id: object.GetId()})
	if err != nil {
		t.Fatalf("ReadBytes(slice) error = %v", err)
	}
	if got := string(bytesResp.GetData()); got != "cde" {
		t.Fatalf("Slice() bytes = %q, want cde", got)
	}
}

func TestOpenReadStreamAndObjectURLLifecycle(t *testing.T) {
	t.Parallel()

	provider, store, now := testProvider(t)
	store.objects["tenant/object"] = fakeObject{
		data:        []byte("stream-me"),
		contentType: "application/octet-stream",
		metadata:    objectMetadata(proto.FileObjectKind_FILE_OBJECT_KIND_BLOB, "", now.UnixMilli()),
		updated:     now,
	}

	stream := &captureReadStream{ctx: context.Background()}
	if err := provider.OpenReadStream(&proto.ReadStreamRequest{Id: "tenant/object"}, stream); err != nil {
		t.Fatalf("OpenReadStream() error = %v", err)
	}
	var joined []byte
	for _, chunk := range stream.chunks {
		joined = append(joined, chunk...)
	}
	if got := string(joined); got != "stream-me" {
		t.Fatalf("OpenReadStream() = %q, want stream-me", got)
	}

	urlResp, err := provider.CreateObjectURL(context.Background(), &proto.CreateObjectURLRequest{Id: "tenant/object"})
	if err != nil {
		t.Fatalf("CreateObjectURL() error = %v", err)
	}
	if urlResp.GetUrl() != objectURLPrefix+"url-1" {
		t.Fatalf("CreateObjectURL() = %q, want %q", urlResp.GetUrl(), objectURLPrefix+"url-1")
	}

	resolved, err := provider.ResolveObjectURL(context.Background(), &proto.ObjectURLRequest{Url: urlResp.GetUrl()})
	if err != nil {
		t.Fatalf("ResolveObjectURL() error = %v", err)
	}
	if resolved.GetObject().GetId() != "tenant/object" {
		t.Fatalf("ResolveObjectURL() id = %q, want tenant/object", resolved.GetObject().GetId())
	}

	if _, err := provider.RevokeObjectURL(context.Background(), &proto.ObjectURLRequest{Url: urlResp.GetUrl()}); err != nil {
		t.Fatalf("RevokeObjectURL() error = %v", err)
	}
	if _, err := provider.RevokeObjectURL(context.Background(), &proto.ObjectURLRequest{Url: urlResp.GetUrl()}); err != nil {
		t.Fatalf("RevokeObjectURL() second call error = %v", err)
	}
	if _, err := provider.ResolveObjectURL(context.Background(), &proto.ObjectURLRequest{Url: urlResp.GetUrl()}); status.Code(err) != codes.NotFound {
		t.Fatalf("ResolveObjectURL() after revoke = %v, want NotFound", err)
	}
}

func TestCloseWithoutConfiguredStore(t *testing.T) {
	t.Parallel()

	var provider Provider
	if err := provider.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
