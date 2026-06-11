package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestPayloadSigningSignedUsesStableSignedPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	payload := []byte("signed payload body")
	requestErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fail := func(format string, args ...any) {
			err := fmt.Errorf(format, args...)
			requestErr <- err
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		if r.Method != http.MethodPut {
			fail("method = %s, want PUT", r.Method)
			return
		}
		if r.URL.Path != "/fixtures/payload-signing.txt" {
			fail("path = %q, want /fixtures/payload-signing.txt", r.URL.Path)
			return
		}
		gotBody, err := io.ReadAll(r.Body)
		if err != nil {
			fail("ReadAll(body): %v", err)
			return
		}
		if !bytes.Equal(gotBody, payload) {
			fail("body = %q, want %q", gotBody, payload)
			return
		}

		wantHash := fmt.Sprintf("%x", sha256.Sum256(payload))
		if got := r.Header.Get("X-Amz-Content-Sha256"); got != wantHash {
			fail("X-Amz-Content-Sha256 = %q, want %q", got, wantHash)
			return
		}
		if got := r.Header.Get(acceptEncodingHeader); got != acceptEncodingIdentity {
			fail("%s = %q, want %q", acceptEncodingHeader, got, acceptEncodingIdentity)
			return
		}
		if got := r.Header.Get(amzSDKInvocationID); got != "" {
			fail("%s = %q, want omitted", amzSDKInvocationID, got)
			return
		}
		if got := r.Header.Get(amzSDKRequestHeader); got != "" {
			fail("%s = %q, want omitted", amzSDKRequestHeader, got)
			return
		}

		signedHeaders, err := signedHeadersFromAuthorization(r.Header.Get("Authorization"))
		if err != nil {
			fail("%v", err)
			return
		}
		for _, check := range []struct {
			header string
			want   bool
		}{
			{header: "x-amz-content-sha256", want: true},
			{header: "accept-encoding", want: false},
			{header: strings.ToLower(amzSDKInvocationID), want: false},
			{header: strings.ToLower(amzSDKRequestHeader), want: false},
		} {
			if err := assertSignedHeader(signedHeaders, check.header, check.want); err != nil {
				fail("%v", err)
				return
			}
		}

		w.Header().Set("ETag", `"payload-signing"`)
		w.WriteHeader(http.StatusOK)
		requestErr <- nil
	}))
	t.Cleanup(server.Close)

	client, _, err := buildS3Client(ctx, config{
		Region:          "us-east-1",
		Endpoint:        server.URL,
		ForcePathStyle:  true,
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		PayloadSigning:  payloadSigningSigned,
	})
	if err != nil {
		t.Fatalf("buildS3Client: %v", err)
	}

	_, err = client.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket:        aws.String("fixtures"),
		Key:           aws.String("payload-signing.txt"),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
		ContentType:   aws.String("text/plain"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	select {
	case err := <-requestErr:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("server did not receive PutObject")
	}
}

func TestPayloadSigningSignedDoesNotForcePresignPutPayloadHash(t *testing.T) {
	t.Parallel()

	_, presigner, err := buildS3Client(context.Background(), config{
		Region:          "us-east-1",
		Endpoint:        "https://example.test",
		ForcePathStyle:  true,
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		PayloadSigning:  payloadSigningSigned,
	})
	if err != nil {
		t.Fatalf("buildS3Client: %v", err)
	}

	presigned, err := presigner.PresignPutObject(context.Background(), &s3sdk.PutObjectInput{
		Bucket:      aws.String("fixtures"),
		Key:         aws.String("upload.txt"),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		t.Fatalf("PresignPutObject: %v", err)
	}
	if got := presigned.SignedHeader.Get("X-Amz-Content-Sha256"); got != "" {
		t.Fatalf("presigned X-Amz-Content-Sha256 header = %q, want omitted", got)
	}
	parsed, err := url.Parse(presigned.URL)
	if err != nil {
		t.Fatalf("url.Parse(presigned.URL): %v", err)
	}
	if signedHeaders := parsed.Query().Get("X-Amz-SignedHeaders"); strings.Contains(signedHeaders, "x-amz-content-sha256") {
		t.Fatalf("presigned SignedHeaders = %q, want no x-amz-content-sha256", signedHeaders)
	}
}

func TestConfigureRejectsUnknownPayloadSigning(t *testing.T) {
	t.Parallel()

	err := New().Configure(context.Background(), "assets", map[string]any{
		"region":         "us-east-1",
		"bucket":         "fixtures",
		"payloadSigning": "required",
	})
	if err == nil {
		t.Fatal("Configure succeeded, want payloadSigning error")
	}
	if !strings.Contains(err.Error(), "payloadSigning") {
		t.Fatalf("Configure error = %v, want payloadSigning", err)
	}
}

func signedHeadersFromAuthorization(auth string) ([]string, error) {
	for _, part := range strings.Split(auth, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "SignedHeaders=") {
			value := strings.TrimPrefix(part, "SignedHeaders=")
			if value == "" {
				return nil, fmt.Errorf("SignedHeaders is empty")
			}
			return strings.Split(value, ";"), nil
		}
	}
	return nil, fmt.Errorf("Authorization missing SignedHeaders: %q", auth)
}

func assertSignedHeader(signedHeaders []string, header string, want bool) error {
	for _, signedHeader := range signedHeaders {
		if signedHeader == header {
			if !want {
				return fmt.Errorf("SignedHeaders contains %q, want omitted: %v", header, signedHeaders)
			}
			return nil
		}
	}
	if want {
		return fmt.Errorf("SignedHeaders missing %q: %v", header, signedHeaders)
	}
	return nil
}
