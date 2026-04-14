package s3_test

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestS3ProviderTransport_WriteReadAndStat(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	key := " docs/" + t.Name() + ".json "
	obj := client.Object(testBackend(t).bucket, key)
	wrote, err := obj.WriteJSON(ctx, map[string]any{
		"ok":   true,
		"name": t.Name(),
	}, &gestalt.WriteOptions{
		ContentType: "application/json",
		Metadata:    map[string]string{"env": "test"},
	})
	if err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	if wrote.Ref.Key != key {
		t.Fatalf("WriteJSON key = %q, want %q", wrote.Ref.Key, key)
	}

	meta, err := obj.Stat(ctx)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if meta.Metadata["env"] != "test" {
		t.Fatalf("Stat metadata env = %q, want test", meta.Metadata["env"])
	}
	if meta.Size <= 0 {
		t.Fatalf("Stat size = %d, want > 0", meta.Size)
	}
	if meta.LastModified.IsZero() {
		t.Fatal("Stat last modified is zero")
	}

	got, err := obj.JSON(ctx, nil)
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	payload := got.(map[string]any)
	if payload["name"] != t.Name() {
		t.Fatalf("JSON name = %v, want %q", payload["name"], t.Name())
	}
}

func TestS3ProviderTransport_StreamedReadAndEmptyObject(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	bucket := testBackend(t).bucket

	blobKey := "chunks/" + t.Name() + ".bin"
	blob := strings.Repeat("abcdef0123456789", 8192)
	obj := client.Object(bucket, blobKey)
	if _, err := obj.WriteString(ctx, blob, &gestalt.WriteOptions{
		ContentType: "application/octet-stream",
	}); err != nil {
		t.Fatalf("WriteString: %v", err)
	}

	meta, body, err := client.ReadObject(ctx, gestalt.ObjectRef{
		Bucket: bucket,
		Key:    blobKey,
	}, nil)
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	defer func() { _ = body.Close() }()
	if meta.Size != int64(len(blob)) {
		t.Fatalf("ReadObject size = %d, want %d", meta.Size, len(blob))
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != blob {
		t.Fatalf("ReadObject body mismatch: got %d bytes", len(data))
	}

	empty := client.Object(bucket, "empty/"+t.Name())
	meta, err = empty.WriteBytes(ctx, nil, &gestalt.WriteOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("WriteBytes(empty): %v", err)
	}
	if meta.Size != 0 {
		t.Fatalf("empty size = %d, want 0", meta.Size)
	}
	text, err := empty.Text(ctx, nil)
	if err != nil {
		t.Fatalf("Text(empty): %v", err)
	}
	if text != "" {
		t.Fatalf("Text(empty) = %q, want empty", text)
	}
}

func TestS3ProviderTransport_RangeReadAndEarlyClose(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	bucket := testBackend(t).bucket

	key := "ranges/" + t.Name() + ".txt"
	payload := strings.Repeat("0123456789", 2048)
	obj := client.Object(bucket, key)
	if _, err := obj.WriteString(ctx, payload, nil); err != nil {
		t.Fatalf("WriteString: %v", err)
	}

	start, end := int64(2), int64(5)
	got, err := obj.Text(ctx, &gestalt.ReadOptions{
		Range: &gestalt.ByteRange{Start: &start, End: &end},
	})
	if err != nil {
		t.Fatalf("Text(range): %v", err)
	}
	if got != "2345" {
		t.Fatalf("Text(range) = %q, want 2345", got)
	}

	endOnly := int64(3)
	got, err = obj.Text(ctx, &gestalt.ReadOptions{
		Range: &gestalt.ByteRange{End: &endOnly},
	})
	if err != nil {
		t.Fatalf("Text(end-only range): %v", err)
	}
	if got != "0123" {
		t.Fatalf("Text(end-only range) = %q, want 0123", got)
	}

	got, err = obj.Text(ctx, &gestalt.ReadOptions{
		Range: &gestalt.ByteRange{},
	})
	if err != nil {
		t.Fatalf("Text(empty range): %v", err)
	}
	if got != payload {
		t.Fatalf("Text(empty range) length = %d, want %d", len(got), len(payload))
	}

	_, body, err := client.ReadObject(ctx, gestalt.ObjectRef{
		Bucket: bucket,
		Key:    key,
	}, nil)
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	buf := make([]byte, 1024)
	n, err := body.Read(buf)
	if err != nil {
		t.Fatalf("Read(first): %v", err)
	}
	if n == 0 {
		t.Fatal("Read(first) returned 0 bytes")
	}
	if err := body.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	n, err = body.Read(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Read(after close) error = %v, want EOF", err)
	}
	if n != 0 {
		t.Fatalf("Read(after close) bytes = %d, want 0", n)
	}
}

func TestS3ProviderTransport_ListCopyDeletePresignAndExists(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	bucket := testBackend(t).bucket

	for _, key := range []string{
		"list/" + t.Name() + "/a.txt",
		"list/" + t.Name() + "/nested/b.txt",
		"list/" + t.Name() + "/nested/c.txt",
		"list/" + t.Name() + "/z.txt",
	} {
		if _, err := client.Object(bucket, key).WriteString(ctx, key, nil); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	basePrefix := "list/" + t.Name() + "/"
	page, err := client.ListObjects(ctx, gestalt.ListOptions{
		Bucket:    bucket,
		Prefix:    basePrefix,
		Delimiter: "/",
	})
	if err != nil {
		t.Fatalf("ListObjects(delimiter): %v", err)
	}
	if len(page.CommonPrefixes) != 1 || page.CommonPrefixes[0] != basePrefix+"nested/" {
		t.Fatalf("CommonPrefixes = %v, want [%s]", page.CommonPrefixes, basePrefix+"nested/")
	}
	if len(page.Objects) != 2 {
		t.Fatalf("Objects(delimiter) len = %d, want 2", len(page.Objects))
	}

	first, err := client.ListObjects(ctx, gestalt.ListOptions{
		Bucket:  bucket,
		Prefix:  basePrefix,
		MaxKeys: 2,
	})
	if err != nil {
		t.Fatalf("ListObjects(first page): %v", err)
	}
	if !first.HasMore {
		t.Fatal("first page HasMore = false, want true")
	}
	second, err := client.ListObjects(ctx, gestalt.ListOptions{
		Bucket:            bucket,
		Prefix:            basePrefix,
		MaxKeys:           2,
		ContinuationToken: first.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjects(second page): %v", err)
	}
	if second.HasMore {
		t.Fatal("second page HasMore = true, want false")
	}
	if len(second.Objects) != 2 {
		t.Fatalf("second page len = %d, want 2", len(second.Objects))
	}

	sourceKey := "copy/" + t.Name() + "/source ?#+.txt"
	source := client.Object(bucket, sourceKey)
	sourceMeta, err := source.WriteString(ctx, "copied", &gestalt.WriteOptions{
		ContentType: "text/plain",
		Metadata:    map[string]string{"copied": "true"},
	})
	if err != nil {
		t.Fatalf("WriteString(source): %v", err)
	}

	destRef := gestalt.ObjectRef{Bucket: bucket, Key: "copy/" + t.Name() + "/dest.txt"}
	dest := client.Object(destRef.Bucket, destRef.Key)
	if _, err := dest.WriteString(ctx, "stale", nil); err != nil {
		t.Fatalf("WriteString(dest seed): %v", err)
	}

	meta, err := client.CopyObject(ctx, gestalt.ObjectRef{
		Bucket: bucket,
		Key:    sourceKey,
	}, destRef, &gestalt.CopyOptions{
		IfMatch: sourceMeta.ETag,
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if meta.Ref.Key != destRef.Key {
		t.Fatalf("CopyObject key = %q, want %q", meta.Ref.Key, destRef.Key)
	}

	exists, err := dest.Exists(ctx)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Fatal("Exists = false, want true")
	}

	text, err := dest.Text(ctx, nil)
	if err != nil {
		t.Fatalf("Text(dest): %v", err)
	}
	if text != "copied" {
		t.Fatalf("Text(dest) = %q, want copied", text)
	}

	presigned, err := dest.Presign(ctx, &gestalt.PresignOptions{
		Method:             gestalt.PresignMethodPut,
		Expires:            15 * time.Minute,
		ContentType:        "text/plain",
		ContentDisposition: `attachment; filename="dest.txt"`,
		Headers:            map[string]string{"x-test": "true"},
	})
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}
	if presigned.Method != gestalt.PresignMethodPut {
		t.Fatalf("Presign method = %q, want PUT", presigned.Method)
	}
	if !strings.Contains(presigned.URL, "X-Amz-Signature=") {
		t.Fatalf("Presign URL = %q, want AWS signature", presigned.URL)
	}
	if presigned.Headers["x-test"] != "true" {
		t.Fatalf("Presign headers = %v", presigned.Headers)
	}
	if got := headerValue(presigned.Headers, "content-type"); got != "text/plain" {
		t.Fatalf("Presign content-type header = %q, want text/plain", got)
	}
	if got := headerValue(presigned.Headers, "content-disposition"); got != `attachment; filename="dest.txt"` {
		t.Fatalf("Presign content-disposition header = %q", got)
	}
	if got := headerValue(presigned.Headers, "host"); got != "" {
		t.Fatalf("Presign host header = %q, want omitted", got)
	}
	if _, err := dest.Presign(ctx, &gestalt.PresignOptions{
		Method:  gestalt.PresignMethodPut,
		Expires: -time.Second,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Presign(negative expiry) error = %v, want InvalidArgument", err)
	}
	getPresigned, err := dest.Presign(ctx, &gestalt.PresignOptions{
		Method:             gestalt.PresignMethodGet,
		ContentType:        "application/octet-stream",
		ContentDisposition: `attachment; filename="download.txt"`,
	})
	if err != nil {
		t.Fatalf("Presign(GET overrides): %v", err)
	}
	parsedGetURL, err := url.Parse(getPresigned.URL)
	if err != nil {
		t.Fatalf("url.Parse(get presign): %v", err)
	}
	if got := parsedGetURL.Query().Get("response-content-type"); got != "application/octet-stream" {
		t.Fatalf("response-content-type query = %q, want application/octet-stream", got)
	}
	if got := parsedGetURL.Query().Get("response-content-disposition"); got != `attachment; filename="download.txt"` {
		t.Fatalf("response-content-disposition query = %q", got)
	}
	if got := headerValue(getPresigned.Headers, "host"); got != "" {
		t.Fatalf("GET presign host header = %q, want omitted", got)
	}

	versioned, err := client.PresignObject(ctx, gestalt.ObjectRef{
		Bucket:    bucket,
		Key:       destRef.Key,
		VersionID: "version id/1",
	}, &gestalt.PresignOptions{
		Method: gestalt.PresignMethodHead,
	})
	if err != nil {
		t.Fatalf("PresignObject(versioned): %v", err)
	}
	parsedPresignURL, err := url.Parse(versioned.URL)
	if err != nil {
		t.Fatalf("url.Parse(versioned presign): %v", err)
	}
	if got := parsedPresignURL.Query().Get("versionId"); got != "version id/1" {
		t.Fatalf("versionId query = %q, want %q", got, "version id/1")
	}

	if err := dest.Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	exists, err = dest.Exists(ctx)
	if err != nil {
		t.Fatalf("Exists(after delete): %v", err)
	}
	if exists {
		t.Fatal("Exists(after delete) = true, want false")
	}
}

func TestS3ProviderTransport_ErrorMapping(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	bucket := testBackend(t).bucket

	missing := client.Object(bucket, "missing/"+t.Name())
	_, err := missing.Stat(ctx)
	if !errors.Is(err, gestalt.ErrS3NotFound) {
		t.Fatalf("Stat missing error = %v, want ErrS3NotFound", err)
	}

	existing := client.Object(bucket, "errors/"+t.Name()+".txt")
	meta, err := existing.WriteString(ctx, "abc", nil)
	if err != nil {
		t.Fatalf("WriteString(existing): %v", err)
	}

	_, err = existing.WriteString(ctx, "overwrite", &gestalt.WriteOptions{
		IfNoneMatch: "*",
	})
	if !errors.Is(err, gestalt.ErrS3PreconditionFailed) {
		t.Fatalf("IfNoneMatch error = %v, want ErrS3PreconditionFailed", err)
	}

	start, end := int64(9), int64(1)
	_, err = existing.Text(ctx, &gestalt.ReadOptions{
		Range: &gestalt.ByteRange{Start: &start, End: &end},
	})
	if !errors.Is(err, gestalt.ErrS3InvalidRange) {
		t.Fatalf("range error = %v, want ErrS3InvalidRange", err)
	}

	negativeStart := int64(-1)
	_, err = existing.Text(ctx, &gestalt.ReadOptions{
		Range: &gestalt.ByteRange{Start: &negativeStart},
	})
	if !errors.Is(err, gestalt.ErrS3InvalidRange) {
		t.Fatalf("negative start range error = %v, want ErrS3InvalidRange", err)
	}

	negativeEnd := int64(-1)
	_, err = existing.Text(ctx, &gestalt.ReadOptions{
		Range: &gestalt.ByteRange{End: &negativeEnd},
	})
	if !errors.Is(err, gestalt.ErrS3InvalidRange) {
		t.Fatalf("negative end range error = %v, want ErrS3InvalidRange", err)
	}

	tooFarStart := int64(1 << 20)
	_, err = existing.Text(ctx, &gestalt.ReadOptions{
		Range: &gestalt.ByteRange{Start: &tooFarStart},
	})
	if !errors.Is(err, gestalt.ErrS3InvalidRange) {
		t.Fatalf("backend invalid range error = %v, want ErrS3InvalidRange", err)
	}

	_, err = existing.Text(ctx, &gestalt.ReadOptions{
		IfNoneMatch: meta.ETag,
	})
	if !errors.Is(err, gestalt.ErrS3PreconditionFailed) {
		t.Fatalf("IfNoneMatch(read) error = %v, want ErrS3PreconditionFailed", err)
	}

	_, err = client.CopyObject(ctx, gestalt.ObjectRef{
		Bucket: bucket,
		Key:    "errors/" + t.Name() + ".txt",
	}, gestalt.ObjectRef{
		Bucket: bucket,
		Key:    "errors/" + t.Name() + "-copy.txt",
	}, &gestalt.CopyOptions{
		IfMatch: "wrong-etag",
	})
	if !errors.Is(err, gestalt.ErrS3PreconditionFailed) {
		t.Fatalf("CopyObject IfMatch error = %v, want ErrS3PreconditionFailed", err)
	}

	_, err = client.CopyObject(ctx, gestalt.ObjectRef{
		Bucket: bucket,
		Key:    "errors/" + t.Name() + ".txt",
	}, gestalt.ObjectRef{
		Bucket: bucket,
		Key:    "errors/" + t.Name() + "-copy-if-none-match.txt",
	}, &gestalt.CopyOptions{
		IfNoneMatch: meta.ETag,
	})
	if !errors.Is(err, gestalt.ErrS3PreconditionFailed) {
		t.Fatalf("CopyObject IfNoneMatch error = %v, want ErrS3PreconditionFailed", err)
	}

	_, err = client.CopyObject(ctx, gestalt.ObjectRef{
		Bucket: bucket,
		Key:    "errors/absent-" + t.Name(),
	}, gestalt.ObjectRef{
		Bucket: bucket,
		Key:    "errors/" + t.Name() + "-copy-2.txt",
	}, nil)
	if !errors.Is(err, gestalt.ErrS3NotFound) {
		t.Fatalf("CopyObject missing error = %v, want ErrS3NotFound", err)
	}

	if meta.ETag == "" {
		t.Fatal("WriteString(existing) ETag is empty")
	}
}

func headerValue(headers map[string]string, key string) string {
	for header, value := range headers {
		if strings.EqualFold(header, key) {
			return value
		}
	}
	return ""
}
