package s3_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	s3provider "github.com/valon-technologies/gestalt-providers/s3/s3"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
)

const minIOImage = "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"

var (
	backendOnce    sync.Once
	backendCfg     s3BackendConfig
	backendErr     error
	backendCleanup func()
)

type skipBackendError struct {
	reason string
}

func (e *skipBackendError) Error() string {
	return e.reason
}

func TestMain(m *testing.M) {
	code := m.Run()
	if backendCleanup != nil {
		backendCleanup()
	}
	os.Exit(code)
}

func newTestClient(t *testing.T) *gestalt.S3Client {
	t.Helper()

	backend := testBackend(t)
	provider := s3provider.New()
	if err := provider.Configure(context.Background(), "assets", map[string]any{
		"region":          backend.region,
		"endpoint":        backend.endpoint,
		"forcePathStyle":  backend.forcePathStyle,
		"accessKeyId":     backend.accessKeyID,
		"secretAccessKey": backend.secretAccessKey,
		"sessionToken":    backend.sessionToken,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	socketPath := newSocketPath(t)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen(unix): %v", err)
	}
	t.Cleanup(func() {
		_ = lis.Close()
		_ = os.Remove(socketPath)
	})

	server := grpc.NewServer()
	proto.RegisterS3Server(server, provider)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.GracefulStop)

	t.Setenv(gestalt.EnvS3Socket, socketPath)
	client, err := gestalt.S3()
	if err != nil {
		t.Fatalf("gestalt.S3(): %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

type s3BackendConfig struct {
	endpoint        string
	region          string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	bucket          string
	forcePathStyle  bool
}

func testBackend(t *testing.T) s3BackendConfig {
	t.Helper()

	backendOnce.Do(func() {
		if cfg, ok := backendFromEnv(); ok {
			backendCfg = cfg
		} else {
			backendCfg, backendCleanup, backendErr = startMinIOBackend(context.Background())
		}
		if backendErr == nil {
			backendErr = ensureBucket(context.Background(), backendCfg)
		}
	})

	if backendErr != nil {
		var skipErr *skipBackendError
		if errors.As(backendErr, &skipErr) {
			t.Skip(skipErr.reason)
		}
		t.Fatalf("start S3 backend: %v", backendErr)
	}
	return backendCfg
}

func backendFromEnv() (s3BackendConfig, bool) {
	endpoint := strings.TrimSpace(os.Getenv("GESTALT_TEST_S3_ENDPOINT"))
	if endpoint == "" {
		return s3BackendConfig{}, false
	}
	accessKeyID := strings.TrimSpace(os.Getenv("GESTALT_TEST_S3_ACCESS_KEY_ID"))
	secretAccessKey := strings.TrimSpace(os.Getenv("GESTALT_TEST_S3_SECRET_ACCESS_KEY"))
	sessionToken := strings.TrimSpace(os.Getenv("GESTALT_TEST_S3_SESSION_TOKEN"))
	if accessKeyID == "" && secretAccessKey == "" && sessionToken == "" {
		accessKeyID = "minioadmin"
		secretAccessKey = "minioadmin"
	}
	return s3BackendConfig{
		endpoint:        endpoint,
		region:          envOrDefault("GESTALT_TEST_S3_REGION", "us-east-1"),
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		sessionToken:    sessionToken,
		bucket:          envOrDefault("GESTALT_TEST_S3_BUCKET", "fixtures"),
		forcePathStyle:  parseBoolEnv("GESTALT_TEST_S3_FORCE_PATH_STYLE", true),
	}, true
}

func startMinIOBackend(ctx context.Context) (s3BackendConfig, func(), error) {
	if err := ensureDockerAvailable(ctx); err != nil {
		return s3BackendConfig{}, nil, err
	}

	port, err := freeTCPPort()
	if err != nil {
		return s3BackendConfig{}, nil, fmt.Errorf("allocate port: %w", err)
	}

	accessKey := envOrDefault("GESTALT_TEST_S3_ACCESS_KEY_ID", "minioadmin")
	secretKey := envOrDefault("GESTALT_TEST_S3_SECRET_ACCESS_KEY", "minioadmin")
	containerName := fmt.Sprintf("gestalt-go-s3-%d-%d", time.Now().UnixNano(), os.Getpid())
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	output, err := execCommand(runCtx, "docker",
		"run", "-d", "--rm",
		"--name", containerName,
		"-e", "MINIO_ROOT_USER="+accessKey,
		"-e", "MINIO_ROOT_PASSWORD="+secretKey,
		"-p", fmt.Sprintf("127.0.0.1:%d:9000", port),
		minIOImage,
		"server", "/data", "--address", ":9000",
	)
	if err != nil {
		return s3BackendConfig{}, nil, fmt.Errorf("docker run minio: %w: %s", err, strings.TrimSpace(output))
	}

	cleanup := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_, _ = execCommand(cleanupCtx, "docker", "rm", "-f", containerName)
	}

	if err := waitForMinIO(endpoint); err != nil {
		cleanup()
		return s3BackendConfig{}, nil, err
	}

	return s3BackendConfig{
		endpoint:        endpoint,
		region:          envOrDefault("GESTALT_TEST_S3_REGION", "us-east-1"),
		accessKeyID:     accessKey,
		secretAccessKey: secretKey,
		sessionToken:    strings.TrimSpace(os.Getenv("GESTALT_TEST_S3_SESSION_TOKEN")),
		bucket:          envOrDefault("GESTALT_TEST_S3_BUCKET", "fixtures"),
		forcePathStyle:  parseBoolEnv("GESTALT_TEST_S3_FORCE_PATH_STYLE", true),
	}, cleanup, nil
}

func ensureDockerAvailable(ctx context.Context) error {
	if _, err := exec.LookPath("docker"); err != nil {
		if runningInCI() {
			return fmt.Errorf("docker is required for S3 integration tests when GESTALT_TEST_S3_ENDPOINT is not set")
		}
		return &skipBackendError{reason: "docker is required for S3 integration tests when GESTALT_TEST_S3_ENDPOINT is not set"}
	}

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if output, err := execCommand(checkCtx, "docker", "info"); err != nil {
		if runningInCI() {
			return fmt.Errorf("docker is unavailable for S3 integration tests: %v (%s)", err, strings.TrimSpace(output))
		}
		return &skipBackendError{reason: fmt.Sprintf("docker is unavailable for S3 integration tests: %v (%s)", err, strings.TrimSpace(output))}
	}
	return nil
}

func waitForMinIO(endpoint string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	healthURL := endpoint + "/minio/health/live"
	for {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, healthURL, nil)
		if err != nil {
			return fmt.Errorf("build MinIO health request: %w", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("wait for MinIO at %s: %w", healthURL, err)
			}
			return fmt.Errorf("wait for MinIO at %s: status %d", healthURL, resp.StatusCode)
		}
		time.Sleep(time.Second)
	}
}

func freeTCPPort() (int, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer lis.Close()
	addr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected listener addr type %T", lis.Addr())
	}
	return addr.Port, nil
}

func execCommand(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func ensureBucket(ctx context.Context, cfg s3BackendConfig) error {
	client, err := rawS3Client(ctx, cfg)
	if err != nil {
		return fmt.Errorf("rawS3Client: %w", err)
	}
	_, err = client.CreateBucket(ctx, &s3sdk.CreateBucketInput{
		Bucket:                    aws.String(cfg.bucket),
		CreateBucketConfiguration: bucketCreateConfig(cfg.region),
	})
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return nil
		}
	}
	if _, headErr := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{
		Bucket: aws.String(cfg.bucket),
	}); headErr == nil {
		return nil
	}
	return fmt.Errorf("CreateBucket(%s): %w", cfg.bucket, err)
}

func bucketCreateConfig(region string) *s3types.CreateBucketConfiguration {
	if region == "" || region == "us-east-1" {
		return nil
	}
	return &s3types.CreateBucketConfiguration{
		LocationConstraint: s3types.BucketLocationConstraint(region),
	}
}

func rawS3Client(ctx context.Context, cfg s3BackendConfig) (*s3sdk.Client, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.region),
	}
	if cfg.accessKeyID != "" || cfg.secretAccessKey != "" || cfg.sessionToken != "" {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.accessKeyID, cfg.secretAccessKey, cfg.sessionToken),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, err
	}
	return s3sdk.NewFromConfig(awsCfg, func(o *s3sdk.Options) {
		o.BaseEndpoint = aws.String(cfg.endpoint)
		o.UsePathStyle = cfg.forcePathStyle
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}), nil
}

func newSocketPath(t *testing.T) string {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "gs3-*.sock")
	if err != nil {
		t.Fatalf("CreateTemp(socket): %v", err)
	}
	socketPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Close(temp socket file): %v", err)
	}
	if err := os.Remove(socketPath); err != nil {
		t.Fatalf("Remove(temp socket file): %v", err)
	}
	return socketPath
}

func envOrDefault(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func parseBoolEnv(name string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch value {
	case "", "default":
		return fallback
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func runningInCI() bool {
	for _, name := range []string{"CI", "GITHUB_ACTIONS", "BUILDKITE"} {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			return true
		}
	}
	return false
}
