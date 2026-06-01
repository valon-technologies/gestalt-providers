package gcs

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/api/option"
)

func TestProvider_Metadata(t *testing.T) {
	provider := New()

	meta := provider.Metadata()
	if meta.Kind != gestalt.ProviderKindS3 {
		t.Fatalf("Metadata kind = %q, want %q", meta.Kind, gestalt.ProviderKindS3)
	}
	if meta.Name != "gcs" {
		t.Fatalf("Metadata name = %q, want gcs", meta.Name)
	}
	if meta.DisplayName == "" || meta.Description == "" || meta.Version == "" {
		t.Fatalf("Metadata has empty display fields: %#v", meta)
	}
}

func TestProvider_HelperMappings(t *testing.T) {
	if got, err := parseGeneration(" 42 ", "ref.versionId"); err != nil || got != 42 {
		t.Fatalf("parseGeneration = %d, %v; want 42, nil", got, err)
	}
	for _, value := range []string{"", "0", "-1", "1.5", "abc"} {
		if _, err := parseGeneration(value, "ref.versionId"); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
			t.Fatalf("parseGeneration(%q) error = %v, want invalid_argument", value, err)
		}
	}

	if got := normalizeKeyPrefix(" /tenant/assets/ "); got != "tenant/assets/" {
		t.Fatalf("normalizeKeyPrefix = %q, want tenant/assets/", got)
	}
	if got := backendKey("tenant/assets/", "docs/file.txt"); got != "tenant/assets/docs/file.txt" {
		t.Fatalf("backendKey = %q", got)
	}
	if got, ok := logicalKey("tenant/assets/", "tenant/assets/docs/file.txt"); !ok || got != "docs/file.txt" {
		t.Fatalf("logicalKey = %q, %v; want docs/file.txt, true", got, ok)
	}
	if _, ok := logicalKey("tenant/assets/", "other/docs/file.txt"); ok {
		t.Fatal("logicalKey mismatched prefix ok = true, want false")
	}

	updated := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	attrs := &storage.ObjectAttrs{
		Name:         "tenant/assets/docs/file.txt",
		Etag:         "etag",
		Size:         123,
		ContentType:  "text/plain",
		Metadata:     map[string]string{"env": "test"},
		StorageClass: "STANDARD",
		Generation:   99,
		Updated:      updated,
	}
	meta := objectMetaFromAttrs("tenant/assets/", attrs)
	if meta.Ref.Key != "docs/file.txt" || meta.Ref.VersionID != "99" {
		t.Fatalf("objectMeta ref = %#v, want docs/file.txt@99", meta.Ref)
	}
	if meta.ETag != "etag" || meta.Size != 123 || meta.ContentType != "text/plain" || meta.LastModified != updated {
		t.Fatalf("objectMeta = %#v", meta)
	}
	attrs.Metadata["env"] = "mutated"
	if meta.Metadata["env"] != "test" {
		t.Fatalf("objectMeta metadata was not cloned: %#v", meta.Metadata)
	}
}

func TestProvider_RangeParams(t *testing.T) {
	tests := []struct {
		name       string
		byteRange  *gestalt.ByteRange
		offset     int64
		length     int64
		ranged     bool
		statusCode gestalt.StatusCode
	}{
		{name: "nil"},
		{name: "empty", byteRange: &gestalt.ByteRange{}},
		{name: "start and end", byteRange: &gestalt.ByteRange{Start: int64Ptr(2), End: int64Ptr(5)}, offset: 2, length: 4, ranged: true},
		{name: "start only", byteRange: &gestalt.ByteRange{Start: int64Ptr(2)}, offset: 2, length: -1, ranged: true},
		{name: "end only", byteRange: &gestalt.ByteRange{End: int64Ptr(5)}, offset: 0, length: 6, ranged: true},
		{name: "negative start", byteRange: &gestalt.ByteRange{Start: int64Ptr(-1)}, statusCode: gestalt.CodeOutOfRange},
		{name: "negative end", byteRange: &gestalt.ByteRange{End: int64Ptr(-1)}, statusCode: gestalt.CodeOutOfRange},
		{name: "inverted", byteRange: &gestalt.ByteRange{Start: int64Ptr(5), End: int64Ptr(2)}, statusCode: gestalt.CodeOutOfRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, length, ranged, err := rangeParams(tt.byteRange)
			if tt.statusCode != "" {
				if !hasStatusCode(err, tt.statusCode) {
					t.Fatalf("rangeParams error = %v, want %s", err, tt.statusCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("rangeParams: %v", err)
			}
			if offset != tt.offset || length != tt.length || ranged != tt.ranged {
				t.Fatalf("rangeParams = (%d, %d, %v), want (%d, %d, %v)", offset, length, ranged, tt.offset, tt.length, tt.ranged)
			}
		})
	}
}

func TestProvider_RejectsUnsupportedConditions(t *testing.T) {
	if err := validateReadRequest(gestalt.ReadRequest{IfMatch: "etag"}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("validateReadRequest(ifMatch) = %v, want invalid_argument", err)
	}
	if err := validateReadRequest(gestalt.ReadRequest{IfNoneMatch: "etag"}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("validateReadRequest(ifNoneMatch) = %v, want invalid_argument", err)
	}
	now := time.Now()
	if err := validateReadRequest(gestalt.ReadRequest{IfModifiedSince: &now}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("validateReadRequest(ifModifiedSince) = %v, want invalid_argument", err)
	}
	if err := validateReadRequest(gestalt.ReadRequest{IfUnmodifiedSince: &now}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("validateReadRequest(ifUnmodifiedSince) = %v, want invalid_argument", err)
	}
	if err := validateCopyRequest(gestalt.CopyRequest{IfMatch: "etag"}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("validateCopyRequest(ifMatch) = %v, want invalid_argument", err)
	}
	if err := validateCopyRequest(gestalt.CopyRequest{IfNoneMatch: "*"}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("validateCopyRequest(ifNoneMatch) = %v, want invalid_argument", err)
	}

	cfg := testConfiguredProvider(t)
	if _, err := writeObjectHandle(cfg, gestalt.ObjectRef{Key: "docs/file.txt"}, &gestalt.WriteRequest{IfMatch: "etag"}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("writeObjectHandle(ifMatch) = %v, want invalid_argument", err)
	}
	if _, err := writeObjectHandle(cfg, gestalt.ObjectRef{Key: "docs/file.txt"}, &gestalt.WriteRequest{IfNoneMatch: "etag"}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("writeObjectHandle(ifNoneMatch etag) = %v, want invalid_argument", err)
	}
	if _, err := writeObjectHandle(cfg, gestalt.ObjectRef{Key: "docs/file.txt", VersionID: "7"}, &gestalt.WriteRequest{IfNoneMatch: "*"}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("writeObjectHandle(version + create-only) = %v, want invalid_argument", err)
	}
	if _, err := writeObjectHandle(cfg, gestalt.ObjectRef{Key: "docs/file.txt"}, &gestalt.WriteRequest{IfNoneMatch: "*"}); err != nil {
		t.Fatalf("writeObjectHandle(create-only): %v", err)
	}
	if _, err := writeObjectHandle(cfg, gestalt.ObjectRef{Key: "docs/file.txt", VersionID: "7"}, nil); err != nil {
		t.Fatalf("writeObjectHandle(generation match): %v", err)
	}
}

func TestProvider_ListStartAfterAndPrefixFiltering(t *testing.T) {
	page := gestalt.ListPage{}
	appendListAttrs(&page, "tenant/assets/", "list/a.txt", []*storage.ObjectAttrs{
		{Name: "tenant/assets/list/a.txt", Generation: 1},
		{Prefix: "tenant/assets/list/nested/"},
		{Name: "tenant/assets/list/z.txt", Generation: 2},
		{Name: "other/list/x.txt", Generation: 3},
	})

	if len(page.CommonPrefixes) != 1 || page.CommonPrefixes[0] != "list/nested/" {
		t.Fatalf("CommonPrefixes = %v, want [list/nested/]", page.CommonPrefixes)
	}
	if len(page.Objects) != 1 || page.Objects[0].Ref.Key != "list/z.txt" || page.Objects[0].Ref.VersionID != "2" {
		t.Fatalf("Objects = %#v, want list/z.txt generation 2", page.Objects)
	}
}

func TestProvider_PresignObjectUsesGCSV4GenerationControls(t *testing.T) {
	ctx := context.Background()
	client := signedURLTestClient(t)
	provider := New()
	provider.name = "assets"
	provider.cfg = config{Bucket: "unit-test-bucket", KeyPrefix: "tenant/assets/"}
	provider.client = client
	provider.bucket = client.Bucket(provider.cfg.Bucket)
	provider.now = func() time.Time {
		return time.Now().UTC().Truncate(time.Second)
	}
	t.Cleanup(func() { _ = provider.Close() })

	if _, err := provider.PresignObject(ctx, gestalt.PresignRequest{
		Ref:     gestalt.ObjectRef{Key: "docs/file.txt"},
		Method:  gestalt.PresignMethodGet,
		Expires: maxPresignTTL + time.Second,
	}); !hasStatusCode(err, gestalt.CodeInvalidArgument) {
		t.Fatalf("PresignObject(long expiry) error = %v, want invalid_argument", err)
	}

	getResult, err := provider.PresignObject(ctx, gestalt.PresignRequest{
		Ref:                gestalt.ObjectRef{Key: "docs/file.txt", VersionID: "123"},
		Method:             gestalt.PresignMethodGet,
		ContentType:        "application/pdf",
		ContentDisposition: `attachment; filename="file.pdf"`,
		Headers:            map[string]string{"Host": "ignore.example"},
	})
	if err != nil {
		t.Fatalf("PresignObject(GET): %v", err)
	}
	getURL, err := url.Parse(getResult.URL)
	if err != nil {
		t.Fatalf("url.Parse(GET): %v", err)
	}
	if getURL.Query().Get("X-Goog-Algorithm") != "GOOG4-RSA-SHA256" {
		t.Fatalf("GET signed URL algorithm = %q", getURL.Query().Get("X-Goog-Algorithm"))
	}
	if got := getURL.Query().Get("generation"); got != "123" {
		t.Fatalf("GET generation query = %q, want 123", got)
	}
	if got := getURL.Query().Get("response-content-type"); got != "application/pdf" {
		t.Fatalf("GET response-content-type = %q, want application/pdf", got)
	}
	if got := getURL.Query().Get("response-content-disposition"); got != `attachment; filename="file.pdf"` {
		t.Fatalf("GET response-content-disposition = %q", got)
	}
	if got := headerValue(getResult.Headers, "host"); got != "" {
		t.Fatalf("GET headers include host = %q, want omitted", got)
	}

	putResult, err := provider.PresignObject(ctx, gestalt.PresignRequest{
		Ref:                gestalt.ObjectRef{Key: "docs/file.txt", VersionID: "456"},
		Method:             gestalt.PresignMethodPut,
		ContentType:        "text/plain",
		ContentDisposition: "inline",
		Headers:            map[string]string{"x-goog-meta-source": "unit", "Host": "ignore.example"},
	})
	if err != nil {
		t.Fatalf("PresignObject(PUT): %v", err)
	}
	putURL, err := url.Parse(putResult.URL)
	if err != nil {
		t.Fatalf("url.Parse(PUT): %v", err)
	}
	if got := putURL.Query().Get("generation"); got != "" {
		t.Fatalf("PUT generation query = %q, want empty", got)
	}
	if got := headerValue(putResult.Headers, "x-goog-if-generation-match"); got != "456" {
		t.Fatalf("PUT x-goog-if-generation-match = %q, want 456", got)
	}
	if got := headerValue(putResult.Headers, "content-type"); got != "text/plain" {
		t.Fatalf("PUT content-type = %q, want text/plain", got)
	}
	if got := headerValue(putResult.Headers, "content-disposition"); got != "inline" {
		t.Fatalf("PUT content-disposition = %q, want inline", got)
	}
	if got := headerValue(putResult.Headers, "host"); got != "" {
		t.Fatalf("PUT headers include host = %q, want omitted", got)
	}
	if !strings.Contains(putURL.Query().Get("X-Goog-SignedHeaders"), "x-goog-if-generation-match") {
		t.Fatalf("PUT signed headers = %q, want generation precondition header signed", putURL.Query().Get("X-Goog-SignedHeaders"))
	}
}

func testConfiguredProvider(t *testing.T) configuredProvider {
	t.Helper()
	client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("storage.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return configuredProvider{
		client:     client,
		bucket:     client.Bucket("unit-test-bucket"),
		bucketName: "unit-test-bucket",
		keyPrefix:  "tenant/assets/",
	}
}

func signedURLTestClient(t *testing.T) *storage.Client {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("x509.MarshalPKCS8PrivateKey: %v", err)
	}
	privateKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	creds, err := json.Marshal(map[string]string{
		"type":                        "service_account",
		"project_id":                  "unit-test-project",
		"private_key_id":              "unit-test-key",
		"private_key":                 string(privateKey),
		"client_email":                "signed-url@example.iam.gserviceaccount.com",
		"client_id":                   "1234567890",
		"auth_uri":                    "https://accounts.google.com/o/oauth2/auth",
		"token_uri":                   "https://oauth2.googleapis.com/token",
		"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
		"client_x509_cert_url":        "https://www.googleapis.com/robot/v1/metadata/x509/signed-url@example.iam.gserviceaccount.com",
	})
	if err != nil {
		t.Fatalf("json.Marshal credentials: %v", err)
	}
	client, err := storage.NewClient(context.Background(), option.WithCredentialsJSON(creds))
	if err != nil {
		t.Fatalf("storage.NewClient: %v", err)
	}
	return client
}

func int64Ptr(value int64) *int64 {
	return &value
}

func writeString(ctx context.Context, provider *Provider, ref gestalt.ObjectRef, body string, opts *gestalt.WriteRequest) (gestalt.ObjectMeta, error) {
	req := gestalt.WriteRequest{Ref: ref, Body: strings.NewReader(body)}
	if opts != nil {
		req = *opts
		req.Ref = ref
		req.Body = strings.NewReader(body)
	}
	return provider.WriteObject(ctx, req)
}

func readText(ctx context.Context, provider *Provider, ref gestalt.ObjectRef, opts *gestalt.ReadRequest) (string, error) {
	data, err := readBytes(ctx, provider, ref, opts)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func readBytes(ctx context.Context, provider *Provider, ref gestalt.ObjectRef, opts *gestalt.ReadRequest) ([]byte, error) {
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
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	return data, nil
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
