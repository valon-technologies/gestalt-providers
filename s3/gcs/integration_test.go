package gcs

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestGCSProvider_IntegrationGenerationPreconditions(t *testing.T) {
	bucket := strings.TrimSpace(os.Getenv("GESTALT_TEST_GCS_BUCKET"))
	if bucket == "" {
		t.Skip("set GESTALT_TEST_GCS_BUCKET to run GCS integration tests")
	}

	ctx := context.Background()
	provider := New()
	keyPrefix := fmt.Sprintf("gestalt-tests/%d/%s/", time.Now().UnixNano(), sanitizeTestName(t.Name()))
	cfg := map[string]any{
		"bucket":    bucket,
		"keyPrefix": keyPrefix,
	}
	if userProject := strings.TrimSpace(os.Getenv("GESTALT_TEST_GCS_USER_PROJECT")); userProject != "" {
		cfg["userProject"] = userProject
	}
	if err := provider.Configure(ctx, "gcs-test", cfg); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	refs := map[string]gestalt.ObjectRef{}
	remember := func(meta gestalt.ObjectMeta) {
		if meta.Ref.Key == "" {
			return
		}
		refs[meta.Ref.Key+"#"+meta.Ref.VersionID] = meta.Ref
	}
	t.Cleanup(func() {
		for _, ref := range refs {
			if err := provider.DeleteObject(context.Background(), ref); err != nil && !hasStatusCode(err, gestalt.CodeNotFound) {
				t.Logf("cleanup delete %s generation %s: %v", ref.Key, ref.VersionID, err)
			}
		}
	})

	sourceRef := gestalt.ObjectRef{Key: "source.txt"}
	first, err := writeString(ctx, provider, sourceRef, "first", &gestalt.WriteRequest{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("WriteObject(first): %v", err)
	}
	remember(first)
	if first.Ref.VersionID == "" {
		t.Fatal("WriteObject(first) versionId is empty, want GCS generation")
	}

	text, err := readText(ctx, provider, first.Ref, nil)
	if err != nil {
		t.Fatalf("ReadObject(first generation): %v", err)
	}
	if text != "first" {
		t.Fatalf("ReadObject(first generation) = %q, want first", text)
	}

	_, err = writeString(ctx, provider, sourceRef, "create-only", &gestalt.WriteRequest{IfNoneMatch: "*"})
	if !hasStatusCode(err, gestalt.CodeFailedPrecondition) {
		t.Fatalf("WriteObject(create-only existing) error = %v, want failed_precondition", err)
	}

	second, err := writeString(ctx, provider, first.Ref, "second", &gestalt.WriteRequest{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("WriteObject(generation match): %v", err)
	}
	remember(second)
	if second.Ref.VersionID == "" || second.Ref.VersionID == first.Ref.VersionID {
		t.Fatalf("WriteObject(generation match) versionId = %q, first = %q", second.Ref.VersionID, first.Ref.VersionID)
	}

	_, err = writeString(ctx, provider, first.Ref, "stale", nil)
	if !hasStatusCode(err, gestalt.CodeFailedPrecondition) {
		t.Fatalf("WriteObject(stale generation) error = %v, want failed_precondition", err)
	}

	destRef := gestalt.ObjectRef{Key: "dest.txt"}
	destSeed, err := writeString(ctx, provider, destRef, "old", &gestalt.WriteRequest{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("WriteObject(dest seed): %v", err)
	}
	remember(destSeed)

	copied, err := provider.CopyObject(ctx, gestalt.CopyRequest{Source: second.Ref, Destination: destSeed.Ref})
	if err != nil {
		t.Fatalf("CopyObject(source generation + destination generation match): %v", err)
	}
	remember(copied)
	if copied.Ref.VersionID == "" || copied.Ref.VersionID == destSeed.Ref.VersionID {
		t.Fatalf("CopyObject versionId = %q, dest seed = %q", copied.Ref.VersionID, destSeed.Ref.VersionID)
	}
	text, err = readText(ctx, provider, copied.Ref, nil)
	if err != nil {
		t.Fatalf("ReadObject(copied generation): %v", err)
	}
	if text != "second" {
		t.Fatalf("ReadObject(copied generation) = %q, want second", text)
	}

	_, err = provider.CopyObject(ctx, gestalt.CopyRequest{Source: second.Ref, Destination: destSeed.Ref})
	if !hasStatusCode(err, gestalt.CodeFailedPrecondition) {
		t.Fatalf("CopyObject(stale destination generation) error = %v, want failed_precondition", err)
	}

	testIntegrationListPaging(t, ctx, provider, remember)
	testIntegrationGzipRangeRead(t, ctx, provider, remember)
}

func sanitizeTestName(name string) string {
	replacer := strings.NewReplacer("/", "-", " ", "-", "_", "-")
	return replacer.Replace(strings.ToLower(name))
}

func testIntegrationListPaging(t *testing.T, ctx context.Context, provider *Provider, remember func(gestalt.ObjectMeta)) {
	t.Helper()

	basePrefix := "list/"
	for _, key := range []string{
		basePrefix + "a.txt",
		basePrefix + "nested/b.txt",
		basePrefix + "nested/c.txt",
		basePrefix + "z.txt",
	} {
		meta, err := writeString(ctx, provider, gestalt.ObjectRef{Key: key}, key, nil)
		if err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
		remember(meta)
	}

	page, err := provider.ListObjects(ctx, gestalt.ListRequest{
		Prefix:     basePrefix,
		Delimiter:  "/",
		StartAfter: basePrefix + "a.txt",
	})
	if err != nil {
		t.Fatalf("ListObjects(delimiter/start-after): %v", err)
	}
	if len(page.CommonPrefixes) != 1 || page.CommonPrefixes[0] != basePrefix+"nested/" {
		t.Fatalf("CommonPrefixes = %v, want [%s]", page.CommonPrefixes, basePrefix+"nested/")
	}
	if len(page.Objects) != 1 || page.Objects[0].Ref.Key != basePrefix+"z.txt" {
		t.Fatalf("Objects = %#v, want [%s]", page.Objects, basePrefix+"z.txt")
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
	if len(first.Objects) != 2 {
		t.Fatalf("first page len = %d, want 2", len(first.Objects))
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
}

func testIntegrationGzipRangeRead(t *testing.T, ctx context.Context, provider *Provider, remember func(gestalt.ObjectMeta)) {
	t.Helper()

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	raw := []byte(strings.Repeat("0123456789", 64))
	if _, err := zw.Write(raw); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	ref := gestalt.ObjectRef{Key: "gzip.txt"}
	meta, err := provider.WriteObject(ctx, gestalt.WriteRequest{
		Ref:             ref,
		Body:            bytes.NewReader(compressed.Bytes()),
		ContentType:     "application/octet-stream",
		ContentEncoding: "gzip",
	})
	if err != nil {
		t.Fatalf("WriteObject(gzip): %v", err)
	}
	remember(meta)

	start, end := int64(2), int64(9)
	got, err := readBytes(ctx, provider, meta.Ref, &gestalt.ReadRequest{
		Range: &gestalt.ByteRange{Start: &start, End: &end},
	})
	if err != nil {
		t.Fatalf("ReadObject(gzip range): %v", err)
	}
	if want := compressed.Bytes()[2:10]; !bytes.Equal(got, want) {
		t.Fatalf("ReadObject(gzip range) = %x, want %x", got, want)
	}
}
