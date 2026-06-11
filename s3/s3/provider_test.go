package s3_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	s3provider "github.com/valon-technologies/gestalt-providers/s3/s3"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	gestalts3 "github.com/valon-technologies/gestalt/sdk/go/s3"
)

func TestS3Provider_WriteReadAndStat(t *testing.T) {
	provider := newTestProvider(t)
	ctx := context.Background()

	key := " docs/" + t.Name() + ".json "
	ref := gestalt.ObjectRef{Key: key}
	wrote, err := writeJSON(ctx, provider, ref, map[string]any{
		"ok":   true,
		"name": t.Name(),
	}, &gestalt.WriteRequest{
		ContentType: "application/json",
		Metadata:    map[string]string{"env": "test"},
	})
	if err != nil {
		t.Fatalf("WriteObject(JSON): %v", err)
	}
	if wrote.Ref.Key != key {
		t.Fatalf("WriteObject key = %q, want %q", wrote.Ref.Key, key)
	}

	meta, err := provider.HeadObject(ctx, ref)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if meta.Metadata["env"] != "test" {
		t.Fatalf("HeadObject metadata env = %q, want test", meta.Metadata["env"])
	}
	if meta.Size <= 0 {
		t.Fatalf("HeadObject size = %d, want > 0", meta.Size)
	}
	if meta.LastModified.IsZero() {
		t.Fatal("HeadObject last modified is zero")
	}

	got, err := readJSON(ctx, provider, ref, nil)
	if err != nil {
		t.Fatalf("ReadObject(JSON): %v", err)
	}
	payload := got.(map[string]any)
	if payload["name"] != t.Name() {
		t.Fatalf("ReadObject JSON name = %v, want %q", payload["name"], t.Name())
	}
}

func TestS3Provider_StreamedReadAndEmptyObject(t *testing.T) {
	provider := newTestProvider(t)
	ctx := context.Background()

	blobKey := "chunks/" + t.Name() + ".bin"
	blob := strings.Repeat("abcdef0123456789", 8192)
	blobRef := gestalt.ObjectRef{Key: blobKey}
	if _, err := writeString(ctx, provider, blobRef, blob, &gestalt.WriteRequest{
		ContentType: "application/octet-stream",
	}); err != nil {
		t.Fatalf("WriteObject(blob): %v", err)
	}

	readResult, err := provider.ReadObject(ctx, gestalt.ReadRequest{Ref: blobRef})
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	meta, body := readResult.Meta, readResult.Body
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

	emptyRef := gestalt.ObjectRef{Key: "empty/" + t.Name()}
	meta, err = writeBytes(ctx, provider, emptyRef, nil, &gestalt.WriteRequest{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("WriteObject(empty): %v", err)
	}
	if meta.Size != 0 {
		t.Fatalf("empty size = %d, want 0", meta.Size)
	}
	text, err := readText(ctx, provider, emptyRef, nil)
	if err != nil {
		t.Fatalf("ReadObject(empty): %v", err)
	}
	if text != "" {
		t.Fatalf("ReadObject(empty) = %q, want empty", text)
	}
}

func TestS3Provider_RangeRead(t *testing.T) {
	provider := newTestProvider(t)
	ctx := context.Background()

	ref := gestalt.ObjectRef{Key: "ranges/" + t.Name() + ".txt"}
	payload := strings.Repeat("0123456789", 2048)
	if _, err := writeString(ctx, provider, ref, payload, nil); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}

	start, end := int64(2), int64(5)
	got, err := readText(ctx, provider, ref, &gestalt.ReadRequest{
		Range: &gestalt.ByteRange{Start: &start, End: &end},
	})
	if err != nil {
		t.Fatalf("ReadObject(range): %v", err)
	}
	if got != "2345" {
		t.Fatalf("ReadObject(range) = %q, want 2345", got)
	}

	endOnly := int64(3)
	got, err = readText(ctx, provider, ref, &gestalt.ReadRequest{
		Range: &gestalt.ByteRange{End: &endOnly},
	})
	if err != nil {
		t.Fatalf("ReadObject(end-only range): %v", err)
	}
	if got != "0123" {
		t.Fatalf("ReadObject(end-only range) = %q, want 0123", got)
	}

	got, err = readText(ctx, provider, ref, &gestalt.ReadRequest{
		Range: &gestalt.ByteRange{},
	})
	if err != nil {
		t.Fatalf("ReadObject(empty range): %v", err)
	}
	if got != payload {
		t.Fatalf("ReadObject(empty range) length = %d, want %d", len(got), len(payload))
	}
}

func TestS3Provider_ListCopyDeletePresignAndExists(t *testing.T) {
	provider := newTestProvider(t)
	ctx := context.Background()

	for _, key := range []string{
		"list/" + t.Name() + "/a.txt",
		"list/" + t.Name() + "/nested/b.txt",
		"list/" + t.Name() + "/nested/c.txt",
		"list/" + t.Name() + "/z.txt",
	} {
		if _, err := writeString(ctx, provider, gestalt.ObjectRef{Key: key}, key, nil); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	basePrefix := "list/" + t.Name() + "/"
	page, err := provider.ListObjects(ctx, gestalt.ListRequest{
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

	first, err := provider.ListObjects(ctx, gestalt.ListRequest{
		Prefix:  basePrefix,
		MaxKeys: 2,
	})
	if err != nil {
		t.Fatalf("ListObjects(first page): %v", err)
	}
	if !first.HasMore {
		t.Fatal("first page HasMore = false, want true")
	}
	second, err := provider.ListObjects(ctx, gestalt.ListRequest{
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
	sourceRef := gestalt.ObjectRef{Key: sourceKey}
	sourceMeta, err := writeString(ctx, provider, sourceRef, "copied", &gestalt.WriteRequest{
		ContentType: "text/plain",
		Metadata:    map[string]string{"copied": "true"},
	})
	if err != nil {
		t.Fatalf("WriteObject(source): %v", err)
	}

	destRef := gestalt.ObjectRef{Key: "copy/" + t.Name() + "/dest.txt"}
	if _, err := writeString(ctx, provider, destRef, "stale", nil); err != nil {
		t.Fatalf("WriteObject(dest seed): %v", err)
	}

	meta, err := provider.CopyObject(ctx, gestalt.CopyRequest{
		Source:      sourceRef,
		Destination: destRef,
		IfMatch:     sourceMeta.ETag,
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if meta.Ref.Key != destRef.Key {
		t.Fatalf("CopyObject key = %q, want %q", meta.Ref.Key, destRef.Key)
	}

	exists, err := objectExists(ctx, provider, destRef)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Fatal("exists = false, want true")
	}

	text, err := readText(ctx, provider, destRef, nil)
	if err != nil {
		t.Fatalf("ReadObject(dest): %v", err)
	}
	if text != "copied" {
		t.Fatalf("ReadObject(dest) = %q, want copied", text)
	}

	presigned, err := provider.PresignObject(ctx, gestalt.PresignRequest{
		Ref:                destRef,
		Method:             gestalts3.PresignMethodPut,
		Expires:            15 * time.Minute,
		ContentType:        "text/plain",
		ContentDisposition: `attachment; filename="dest.txt"`,
		Headers:            map[string]string{"x-test": "true"},
	})
	if err != nil {
		t.Fatalf("PresignObject(PUT): %v", err)
	}
	if presigned.Method != gestalts3.PresignMethodPut {
		t.Fatalf("Presign method = %q, want PUT", presigned.Method)
	}
	if !strings.Contains(presigned.URL, "X-Amz-Signature=") {
		t.Fatalf("Presign URL = %q, want S3-compatible signature", presigned.URL)
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
	if _, err := provider.PresignObject(ctx, gestalt.PresignRequest{
		Ref:     destRef,
		Method:  gestalts3.PresignMethodPut,
		Expires: -time.Second,
	}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("PresignObject(negative expiry) error = %v, want invalid_argument", err)
	}
	getPresigned, err := provider.PresignObject(ctx, gestalt.PresignRequest{
		Ref:                destRef,
		Method:             gestalts3.PresignMethodGet,
		ContentType:        "application/octet-stream",
		ContentDisposition: `attachment; filename="download.txt"`,
	})
	if err != nil {
		t.Fatalf("PresignObject(GET overrides): %v", err)
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

	versioned, err := provider.PresignObject(ctx, gestalt.PresignRequest{
		Ref:    gestalt.ObjectRef{Key: destRef.Key, VersionID: "version id/1"},
		Method: gestalts3.PresignMethodHead,
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

	if err := provider.DeleteObject(ctx, destRef); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	exists, err = objectExists(ctx, provider, destRef)
	if err != nil {
		t.Fatalf("exists after delete: %v", err)
	}
	if exists {
		t.Fatal("exists after delete = true, want false")
	}
}

func TestS3Provider_KeyPrefixIsTransparent(t *testing.T) {
	normalizedKeyPrefix := "tenant key prefix ?#+/" + t.Name() + "/"
	provider := newTestProvider(t, "/"+strings.TrimSuffix(normalizedKeyPrefix, "/")+"/")
	ctx := context.Background()

	sourceRef := gestalt.ObjectRef{Key: "objects/" + t.Name() + "/source ?#+.txt"}
	if _, err := writeString(ctx, provider, sourceRef, "prefixed", &gestalt.WriteRequest{
		ContentType: "text/plain",
		Metadata:    map[string]string{"prefix": "true"},
	}); err != nil {
		t.Fatalf("WriteObject(source): %v", err)
	}

	destRef := gestalt.ObjectRef{Key: "objects/" + t.Name() + "/dest ?#+.txt"}
	if _, err := provider.CopyObject(ctx, gestalt.CopyRequest{Source: sourceRef, Destination: destRef}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	destText, err := readText(ctx, provider, destRef, nil)
	if err != nil {
		t.Fatalf("ReadObject(dest): %v", err)
	}
	if destText != "prefixed" {
		t.Fatalf("ReadObject(dest) = %q, want prefixed", destText)
	}

	presigned, err := provider.PresignObject(ctx, gestalt.PresignRequest{Ref: destRef, Method: gestalts3.PresignMethodGet})
	if err != nil {
		t.Fatalf("PresignObject(GET): %v", err)
	}
	parsed, err := url.Parse(presigned.URL)
	if err != nil {
		t.Fatalf("url.Parse(presign): %v", err)
	}
	presignedPath, err := url.PathUnescape(parsed.EscapedPath())
	if err != nil {
		t.Fatalf("PathUnescape(presign path): %v", err)
	}
	if !strings.HasSuffix(presignedPath, "/"+normalizedKeyPrefix+destRef.Key) {
		t.Fatalf("presign path = %q, want suffix %q", presignedPath, "/"+normalizedKeyPrefix+destRef.Key)
	}

	basePrefix := "list/" + t.Name() + "/"
	for _, key := range []string{
		basePrefix + "a.txt",
		basePrefix + "nested/b.txt",
		basePrefix + "z.txt",
	} {
		if _, err := writeString(ctx, provider, gestalt.ObjectRef{Key: key}, key, nil); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	page, err := provider.ListObjects(ctx, gestalt.ListRequest{
		Prefix:     basePrefix,
		Delimiter:  "/",
		StartAfter: basePrefix + "a.txt",
	})
	if err != nil {
		t.Fatalf("ListObjects(start after): %v", err)
	}
	if len(page.CommonPrefixes) != 1 || page.CommonPrefixes[0] != basePrefix+"nested/" {
		t.Fatalf("CommonPrefixes = %v, want [%s]", page.CommonPrefixes, basePrefix+"nested/")
	}
	if len(page.Objects) != 1 || page.Objects[0].Ref.Key != basePrefix+"z.txt" {
		t.Fatalf("Objects = %v, want [%s]", page.Objects, basePrefix+"z.txt")
	}
}

func TestS3Provider_StatusMapping(t *testing.T) {
	provider := newTestProvider(t)
	ctx := context.Background()

	missingRef := gestalt.ObjectRef{Key: "missing/" + t.Name()}
	_, err := provider.HeadObject(ctx, missingRef)
	requireStatusCode(t, err, gestalt.CodeNotFound)

	existingRef := gestalt.ObjectRef{Key: "errors/" + t.Name() + ".txt"}
	meta, err := writeString(ctx, provider, existingRef, "abc", nil)
	if err != nil {
		t.Fatalf("WriteObject(existing): %v", err)
	}

	_, err = writeString(ctx, provider, existingRef, "overwrite", &gestalt.WriteRequest{
		IfNoneMatch: "*",
	})
	requireStatusCode(t, err, gestalt.CodeFailedPrecondition)

	start, end := int64(9), int64(1)
	_, err = readText(ctx, provider, existingRef, &gestalt.ReadRequest{
		Range: &gestalt.ByteRange{Start: &start, End: &end},
	})
	requireStatusCode(t, err, gestalt.CodeOutOfRange)

	negativeStart := int64(-1)
	_, err = readText(ctx, provider, existingRef, &gestalt.ReadRequest{
		Range: &gestalt.ByteRange{Start: &negativeStart},
	})
	requireStatusCode(t, err, gestalt.CodeOutOfRange)

	negativeEnd := int64(-1)
	_, err = readText(ctx, provider, existingRef, &gestalt.ReadRequest{
		Range: &gestalt.ByteRange{End: &negativeEnd},
	})
	requireStatusCode(t, err, gestalt.CodeOutOfRange)

	tooFarStart := int64(1 << 20)
	_, err = readText(ctx, provider, existingRef, &gestalt.ReadRequest{
		Range: &gestalt.ByteRange{Start: &tooFarStart},
	})
	requireStatusCode(t, err, gestalt.CodeOutOfRange)

	_, err = readText(ctx, provider, existingRef, &gestalt.ReadRequest{
		IfNoneMatch: meta.ETag,
	})
	requireStatusCode(t, err, gestalt.CodeFailedPrecondition)

	_, err = provider.CopyObject(ctx, gestalt.CopyRequest{
		Source:      existingRef,
		Destination: gestalt.ObjectRef{Key: "errors/" + t.Name() + "-copy.txt"},
		IfMatch:     "wrong-etag",
	})
	requireStatusCode(t, err, gestalt.CodeFailedPrecondition)

	_, err = provider.CopyObject(ctx, gestalt.CopyRequest{
		Source:      existingRef,
		Destination: gestalt.ObjectRef{Key: "errors/" + t.Name() + "-copy-if-none-match.txt"},
		IfNoneMatch: meta.ETag,
	})
	requireStatusCode(t, err, gestalt.CodeFailedPrecondition)

	_, err = provider.CopyObject(ctx, gestalt.CopyRequest{
		Source:      gestalt.ObjectRef{Key: "errors/absent-" + t.Name()},
		Destination: gestalt.ObjectRef{Key: "errors/" + t.Name() + "-copy-2.txt"},
	})
	requireStatusCode(t, err, gestalt.CodeNotFound)

	if meta.ETag == "" {
		t.Fatal("WriteObject(existing) ETag is empty")
	}
}

func writeString(ctx context.Context, provider *s3provider.Provider, ref gestalt.ObjectRef, body string, opts *gestalt.WriteRequest) (gestalt.ObjectMeta, error) {
	return writeBytes(ctx, provider, ref, []byte(body), opts)
}

func writeBytes(ctx context.Context, provider *s3provider.Provider, ref gestalt.ObjectRef, body []byte, opts *gestalt.WriteRequest) (gestalt.ObjectMeta, error) {
	req := gestalt.WriteRequest{Ref: ref, Body: bytes.NewReader(body)}
	if opts != nil {
		req = *opts
		req.Ref = ref
		req.Body = bytes.NewReader(body)
	}
	return provider.WriteObject(ctx, req)
}

func writeJSON(ctx context.Context, provider *s3provider.Provider, ref gestalt.ObjectRef, value any, opts *gestalt.WriteRequest) (gestalt.ObjectMeta, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	if opts == nil {
		opts = &gestalt.WriteRequest{ContentType: "application/json"}
	} else if opts.ContentType == "" {
		copied := *opts
		copied.ContentType = "application/json"
		opts = &copied
	}
	return writeBytes(ctx, provider, ref, body, opts)
}

func readText(ctx context.Context, provider *s3provider.Provider, ref gestalt.ObjectRef, opts *gestalt.ReadRequest) (string, error) {
	data, err := readBytes(ctx, provider, ref, opts)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func readBytes(ctx context.Context, provider *s3provider.Provider, ref gestalt.ObjectRef, opts *gestalt.ReadRequest) ([]byte, error) {
	req := gestalt.ReadRequest{Ref: ref}
	if opts != nil {
		req = *opts
		req.Ref = ref
	}
	result, err := provider.ReadObject(ctx, req)
	if err != nil {
		return nil, err
	}
	body := result.Body
	defer func() { _ = body.Close() }()
	return io.ReadAll(body)
}

func readJSON(ctx context.Context, provider *s3provider.Provider, ref gestalt.ObjectRef, opts *gestalt.ReadRequest) (any, error) {
	data, err := readBytes(ctx, provider, ref, opts)
	if err != nil {
		return nil, err
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func objectExists(ctx context.Context, provider *s3provider.Provider, ref gestalt.ObjectRef) (bool, error) {
	_, err := provider.HeadObject(ctx, ref)
	if err == nil {
		return true, nil
	}
	if hasStatusCode(err, gestalt.CodeNotFound) {
		return false, nil
	}
	return false, err
}

func requireStatusCode(t *testing.T, err error, want gestalt.StatusCode) {
	t.Helper()
	if !hasStatusCode(err, want) {
		code, ok := gestalt.StatusCodeOf(err)
		t.Fatalf("status code = %q (ok=%v), want %q; err=%v", code, ok, want, err)
	}
}

func hasStatusCode(err error, want gestalt.StatusCode) bool {
	code, ok := gestalt.StatusCodeOf(err)
	return ok && code == want
}

func headerValue(headers map[string]string, key string) string {
	for header, value := range headers {
		if strings.EqualFold(header, key) {
			return value
		}
	}
	return ""
}
