package openfga

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	fgaclient "github.com/openfga/go-sdk/client"
	sdkcredentials "github.com/openfga/go-sdk/credentials"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const openFGAImage = "openfga/openfga:v1.14.2"

var (
	backendOnce    sync.Once
	backendCfg     openFGABackendConfig
	backendErr     error
	backendCleanup func()
)

type openFGABackendConfig struct {
	apiURL   string
	apiToken string
}

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

func TestAuthorizationProviderRoundTrip(t *testing.T) {
	sess := newProviderSession(t)

	meta, err := sess.runtime.GetProviderIdentity(sess.ctx, &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("GetProviderIdentity: %v", err)
	}
	if meta.GetKind() != proto.ProviderKind_PROVIDER_KIND_AUTHORIZATION {
		t.Fatalf("kind = %v, want AUTHORIZATION", meta.GetKind())
	}
	if meta.GetName() != "openfga" {
		t.Fatalf("name = %q, want openfga", meta.GetName())
	}

	sess.configure(t)

	modelRef, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel: %v", err)
	}
	if modelRef.GetId() == "" {
		t.Fatal("WriteModel returned empty id")
	}
	if modelRef.GetVersion() != "1" {
		t.Fatalf("model version = %q, want 1", modelRef.GetVersion())
	}

	authzMeta, err := sess.client.GetMetadata(sess.ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	if authzMeta.GetActiveModelId() != modelRef.GetId() {
		t.Fatalf("active_model_id = %q, want %q", authzMeta.GetActiveModelId(), modelRef.GetId())
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "editor",
				Resource: &proto.Resource{Type: "document", Id: "doc-2"},
			},
			{
				Subject:  &proto.Subject{Type: "user", Id: "bob"},
				Relation: "editor",
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("WriteRelationships: %v", err)
	}

	allowed, err := sess.client.Evaluate(sess.ctx, &proto.AccessEvaluationRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Action:   &proto.Action{Name: "read"},
		Resource: &proto.Resource{Type: "document", Id: "doc-1"},
	})
	if err != nil {
		t.Fatalf("Evaluate(read): %v", err)
	}
	if !allowed.GetAllowed() {
		t.Fatal("Evaluate(read) = false, want true")
	}
	if allowed.GetModelId() != modelRef.GetId() {
		t.Fatalf("decision model_id = %q, want %q", allowed.GetModelId(), modelRef.GetId())
	}

	denied, err := sess.client.Evaluate(sess.ctx, &proto.AccessEvaluationRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Action:   &proto.Action{Name: "write"},
		Resource: &proto.Resource{Type: "document", Id: "doc-1"},
	})
	if err != nil {
		t.Fatalf("Evaluate(write): %v", err)
	}
	if denied.GetAllowed() {
		t.Fatal("Evaluate(write) = true, want false")
	}

	many, err := sess.client.EvaluateMany(sess.ctx, &proto.AccessEvaluationsRequest{
		Requests: []*proto.AccessEvaluationRequest{
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Action:   &proto.Action{Name: "read"},
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Action:   &proto.Action{Name: "write"},
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("EvaluateMany: %v", err)
	}
	if len(many.GetDecisions()) != 2 || !many.GetDecisions()[0].GetAllowed() || many.GetDecisions()[1].GetAllowed() {
		t.Fatalf("EvaluateMany decisions = %#v", many.GetDecisions())
	}

	resourceSearch, err := sess.client.SearchResources(sess.ctx, &proto.ResourceSearchRequest{
		Subject:      &proto.Subject{Type: "user", Id: "alice"},
		Action:       &proto.Action{Name: "read"},
		ResourceType: "document",
	})
	if err != nil {
		t.Fatalf("SearchResources: %v", err)
	}
	if got := resourceIDs(resourceSearch.GetResources()); !reflect.DeepEqual(got, []string{"doc-1", "doc-2"}) {
		t.Fatalf("SearchResources ids = %#v, want %#v", got, []string{"doc-1", "doc-2"})
	}

	subjectSearch, err := sess.client.SearchSubjects(sess.ctx, &proto.SubjectSearchRequest{
		Resource:    &proto.Resource{Type: "document", Id: "doc-1"},
		Action:      &proto.Action{Name: "write"},
		SubjectType: "user",
	})
	if err != nil {
		t.Fatalf("SearchSubjects: %v", err)
	}
	if got := subjectIDs(subjectSearch.GetSubjects()); !reflect.DeepEqual(got, []string{"bob"}) {
		t.Fatalf("SearchSubjects ids = %#v, want %#v", got, []string{"bob"})
	}

	noSubjects, err := sess.client.SearchSubjects(sess.ctx, &proto.SubjectSearchRequest{
		Resource:    &proto.Resource{Type: "document", Id: "doc-1"},
		Action:      &proto.Action{Name: "write"},
		SubjectType: "service",
	})
	if err != nil {
		t.Fatalf("SearchSubjects(filtered): %v", err)
	}
	if got := subjectIDs(noSubjects.GetSubjects()); len(got) != 0 {
		t.Fatalf("SearchSubjects filtered ids = %#v, want empty", got)
	}

	actionSearch, err := sess.client.SearchActions(sess.ctx, &proto.ActionSearchRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Resource: &proto.Resource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("SearchActions: %v", err)
	}
	if got := actionNames(actionSearch.GetActions()); !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("SearchActions actions = %#v, want %#v", got, []string{"read", "write"})
	}

	readResp, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Subject: &proto.Subject{Type: "user", Id: "alice"},
	})
	if err != nil {
		t.Fatalf("ReadRelationships: %v", err)
	}
	if got := relationshipPairs(readResp.GetRelationships()); !reflect.DeepEqual(got, []string{"viewer:document/doc-1", "editor:document/doc-2"}) {
		t.Fatalf("ReadRelationships pairs = %#v", got)
	}

	active, err := sess.client.GetActiveModel(sess.ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetActiveModel: %v", err)
	}
	if active.GetModel().GetId() != modelRef.GetId() {
		t.Fatalf("active model id = %q, want %q", active.GetModel().GetId(), modelRef.GetId())
	}

	models, err := sess.client.ListModels(sess.ctx, &proto.ListModelsRequest{})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models.GetModels()) != 1 || models.GetModels()[0].GetId() != modelRef.GetId() {
		t.Fatalf("ListModels = %#v", models.GetModels())
	}

	rotatedModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: rotatedModel()})
	if err != nil {
		t.Fatalf("WriteModel(rotated): %v", err)
	}
	if rotatedModel.GetId() == "" {
		t.Fatal("WriteModel(rotated) returned empty id")
	}

	staleActionSearch, err := sess.client.SearchActions(sess.ctx, &proto.ActionSearchRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Resource: &proto.Resource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("SearchActions(stale): %v", err)
	}
	if got := actionNames(staleActionSearch.GetActions()); len(got) != 0 {
		t.Fatalf("SearchActions stale actions = %#v, want empty", got)
	}

	rotatedDecision, err := sess.client.Evaluate(sess.ctx, &proto.AccessEvaluationRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Action:   &proto.Action{Name: "write"},
		Resource: &proto.Resource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("Evaluate(rotated): %v", err)
	}
	if rotatedDecision.GetAllowed() {
		t.Fatal("Evaluate(rotated) = true, want false")
	}

	reactivatedModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(reactivated): %v", err)
	}
	if reactivatedModel.GetId() != modelRef.GetId() {
		t.Fatalf("reactivated model id = %q, want %q", reactivatedModel.GetId(), modelRef.GetId())
	}
	if !reactivatedModel.GetCreatedAt().AsTime().Equal(modelRef.GetCreatedAt().AsTime()) {
		t.Fatalf("reactivated model created_at = %v, want %v", reactivatedModel.GetCreatedAt(), modelRef.GetCreatedAt())
	}

	reactivatedActionSearch, err := sess.client.SearchActions(sess.ctx, &proto.ActionSearchRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Resource: &proto.Resource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("SearchActions(reactivated): %v", err)
	}
	if got := actionNames(reactivatedActionSearch.GetActions()); !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("SearchActions reactivated actions = %#v, want %#v", got, []string{"read", "write"})
	}

	reactivatedDecision, err := sess.client.Evaluate(sess.ctx, &proto.AccessEvaluationRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Action:   &proto.Action{Name: "write"},
		Resource: &proto.Resource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("Evaluate(reactivated): %v", err)
	}
	if !reactivatedDecision.GetAllowed() {
		t.Fatal("Evaluate(reactivated) = false, want true")
	}
}

func TestAuthorizationProviderValidationAndPagination(t *testing.T) {
	sess := newProviderSession(t)
	sess.configure(t)

	firstModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(first): %v", err)
	}
	secondModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: expandedModel()})
	if err != nil {
		t.Fatalf("WriteModel(second): %v", err)
	}
	if firstModel.GetId() == secondModel.GetId() {
		t.Fatal("expected distinct model ids")
	}

	firstPage, err := sess.client.ListModels(sess.ctx, &proto.ListModelsRequest{PageSize: 1})
	if err != nil {
		t.Fatalf("ListModels(first page): %v", err)
	}
	if len(firstPage.GetModels()) != 1 || firstPage.GetNextPageToken() == "" {
		t.Fatalf("first ListModels page = %#v", firstPage)
	}
	secondPage, err := sess.client.ListModels(sess.ctx, &proto.ListModelsRequest{
		PageSize:  1,
		PageToken: firstPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ListModels(second page): %v", err)
	}
	if len(secondPage.GetModels()) != 1 {
		t.Fatalf("second ListModels page = %#v", secondPage)
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{
			{
				Subject:  &proto.Subject{Type: "service", Id: "worker-1"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteRelationships invalid subject type code = %v, want INVALID_ARGUMENT", err)
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{
			{
				Subject: &proto.Subject{
					Type:       "user",
					Id:         "alice",
					Properties: mustStruct(t, map[string]any{"email": "alice@example.test"}),
				},
				Relation: "viewer",
				Resource: &proto.Resource{
					Type:       "document",
					Id:         "doc-props",
					Properties: mustStruct(t, map[string]any{"title": "Roadmap"}),
				},
				Properties: mustStruct(t, map[string]any{"source": "import"}),
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteRelationships property payload code = %v, want INVALID_ARGUMENT", err)
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "editor",
				Resource: &proto.Resource{Type: "document", Id: "doc-2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("WriteRelationships(valid): %v", err)
	}

	readPage, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("ReadRelationships(first page): %v", err)
	}
	if len(readPage.GetRelationships()) != 1 || readPage.GetNextPageToken() == "" {
		t.Fatalf("first ReadRelationships page = %#v", readPage)
	}
	nextPage, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Subject:   &proto.Subject{Type: "user", Id: "alice"},
		PageSize:  1,
		PageToken: readPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ReadRelationships(second page): %v", err)
	}
	if len(nextPage.GetRelationships()) != 1 {
		t.Fatalf("second ReadRelationships page = %#v", nextPage)
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Deletes: []*proto.RelationshipKey{
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
		},
		Writes: []*proto.Relationship{
			{
				Subject:  &proto.Subject{Type: "service", Id: "worker-1"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "document", Id: "doc-9"},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteRelationships mixed batch code = %v, want INVALID_ARGUMENT", err)
	}

	atomicRead, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Subject: &proto.Subject{Type: "user", Id: "alice"},
	})
	if err != nil {
		t.Fatalf("ReadRelationships(after failed batch): %v", err)
	}
	if got := relationshipPairs(atomicRead.GetRelationships()); !reflect.DeepEqual(got, []string{"viewer:document/doc-1", "editor:document/doc-2"}) {
		t.Fatalf("ReadRelationships after failed batch = %#v", got)
	}
}

func TestAuthorizationProviderWriteModelIsIdempotent(t *testing.T) {
	sess := newProviderSession(t)
	sess.configure(t)

	firstModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(first): %v", err)
	}
	secondModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(second): %v", err)
	}
	if firstModel.GetId() != secondModel.GetId() {
		t.Fatalf("WriteModel id mismatch: first=%q second=%q", firstModel.GetId(), secondModel.GetId())
	}
	if !firstModel.GetCreatedAt().AsTime().Equal(secondModel.GetCreatedAt().AsTime()) {
		t.Fatalf("WriteModel created_at mismatch: first=%v second=%v", firstModel.GetCreatedAt(), secondModel.GetCreatedAt())
	}

	models, err := sess.client.ListModels(sess.ctx, &proto.ListModelsRequest{})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models.GetModels()) != 1 {
		t.Fatalf("ListModels count = %d, want 1", len(models.GetModels()))
	}
}

type providerSession struct {
	ctx        context.Context
	cancel     context.CancelFunc
	backend    openFGABackendConfig
	storeID    string
	runtime    proto.ProviderLifecycleClient
	client     proto.AuthorizationProviderClient
	authzErrCh chan error
}

func newProviderSession(t *testing.T) *providerSession {
	t.Helper()

	backend := testBackend(t)
	storeID := createTestStore(t, backend)

	authzSocket := newSocketPath(t, "authorization.sock")
	t.Setenv(proto.EnvProviderSocket, authzSocket)
	authzProvider := New()
	authzCtx, authzCancel := context.WithCancel(context.Background())
	authzErrCh := make(chan error, 1)
	go func() {
		authzErrCh <- gestalt.ServeAuthorizationProvider(authzCtx, authzProvider)
	}()

	conn := newUnixConn(t, authzSocket)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	session := &providerSession{
		ctx:        ctx,
		cancel:     cancel,
		backend:    backend,
		storeID:    storeID,
		runtime:    proto.NewProviderLifecycleClient(conn),
		client:     proto.NewAuthorizationProviderClient(conn),
		authzErrCh: authzErrCh,
	}
	t.Cleanup(func() {
		cancel()
		authzCancel()
		waitServeResult(t, authzErrCh)
		_ = conn.Close()
		deleteTestStore(context.Background(), backend, storeID)
	})
	return session
}

func (s *providerSession) configure(t *testing.T) {
	t.Helper()
	config := map[string]any{
		"apiUrl":  s.backend.apiURL,
		"storeId": s.storeID,
	}
	if s.backend.apiToken != "" {
		config["apiToken"] = s.backend.apiToken
	}
	cfg, err := structpb.NewStruct(config)
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	_, err = s.runtime.ConfigureProvider(s.ctx, &proto.ConfigureProviderRequest{
		Name:            "authz-openfga",
		Config:          cfg,
		ProtocolVersion: proto.CurrentProtocolVersion,
	})
	if err != nil {
		t.Fatalf("ConfigureProvider: %v", err)
	}
}

func testBackend(t *testing.T) openFGABackendConfig {
	t.Helper()

	backendOnce.Do(func() {
		if cfg, ok := backendFromEnv(); ok {
			backendCfg = cfg
		} else {
			backendCfg, backendCleanup, backendErr = startOpenFGABackend(context.Background())
		}
	})

	if backendErr != nil {
		var skipErr *skipBackendError
		if errors.As(backendErr, &skipErr) {
			t.Skip(skipErr.reason)
		}
		t.Fatalf("start OpenFGA backend: %v", backendErr)
	}
	return backendCfg
}

func backendFromEnv() (openFGABackendConfig, bool) {
	apiURL := strings.TrimSpace(os.Getenv("GESTALT_TEST_OPENFGA_API_URL"))
	if apiURL == "" {
		return openFGABackendConfig{}, false
	}
	return openFGABackendConfig{
		apiURL:   apiURL,
		apiToken: strings.TrimSpace(os.Getenv("GESTALT_TEST_OPENFGA_API_TOKEN")),
	}, true
}

func startOpenFGABackend(ctx context.Context) (openFGABackendConfig, func(), error) {
	if err := ensureDockerAvailable(ctx); err != nil {
		return openFGABackendConfig{}, nil, err
	}

	port, err := freeTCPPort()
	if err != nil {
		return openFGABackendConfig{}, nil, fmt.Errorf("allocate port: %w", err)
	}
	containerName := fmt.Sprintf("gestalt-openfga-%d-%d", time.Now().UnixNano(), os.Getpid())
	apiURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	output, err := execCommand(runCtx, "docker",
		"run", "-d", "--rm",
		"--name", containerName,
		"-p", fmt.Sprintf("127.0.0.1:%d:8080", port),
		openFGAImage,
		"run", "--experimentals=enable-list-users",
	)
	if err != nil {
		return openFGABackendConfig{}, nil, fmt.Errorf("docker run openfga: %w: %s", err, strings.TrimSpace(output))
	}

	cleanup := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_, _ = execCommand(cleanupCtx, "docker", "rm", "-f", containerName)
	}
	if err := waitForOpenFGA(apiURL); err != nil {
		cleanup()
		return openFGABackendConfig{}, nil, err
	}

	return openFGABackendConfig{apiURL: apiURL}, cleanup, nil
}

func waitForOpenFGA(apiURL string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	healthURL := apiURL + "/healthz"
	for {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, healthURL, nil)
		if err != nil {
			return fmt.Errorf("build OpenFGA health request: %w", err)
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
				return fmt.Errorf("wait for OpenFGA at %s: %w", healthURL, err)
			}
			return fmt.Errorf("wait for OpenFGA at %s: status %d", healthURL, resp.StatusCode)
		}
		time.Sleep(time.Second)
	}
}

func createTestStore(t *testing.T, backend openFGABackendConfig) string {
	t.Helper()

	client := newRootClient(t, backend)
	resp, err := client.CreateStore(context.Background()).Body(fgaclient.ClientCreateStoreRequest{
		Name: fmt.Sprintf("gestalt-authz-%d", time.Now().UnixNano()),
	}).Execute()
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	if resp.GetId() == "" {
		t.Fatal("CreateStore returned empty id")
	}
	return resp.GetId()
}

func deleteTestStore(ctx context.Context, backend openFGABackendConfig, storeID string) {
	if strings.TrimSpace(storeID) == "" {
		return
	}
	client, err := rootClient(backend)
	if err != nil {
		return
	}
	_, _ = client.DeleteStore(ctx).Options(fgaclient.ClientDeleteStoreOptions{
		StoreId: &storeID,
	}).Execute()
}

func newRootClient(t *testing.T, backend openFGABackendConfig) *fgaclient.OpenFgaClient {
	t.Helper()
	client, err := rootClient(backend)
	if err != nil {
		t.Fatalf("rootClient: %v", err)
	}
	return client
}

func rootClient(backend openFGABackendConfig) (*fgaclient.OpenFgaClient, error) {
	clientCfg := &fgaclient.ClientConfiguration{ApiUrl: backend.apiURL}
	if backend.apiToken != "" {
		creds, err := sdkcredentials.NewCredentials(sdkcredentials.Credentials{
			Method: sdkcredentials.CredentialsMethodApiToken,
			Config: &sdkcredentials.Config{ApiToken: backend.apiToken},
		})
		if err != nil {
			return nil, err
		}
		clientCfg.Credentials = creds
	}
	return fgaclient.NewSdkClient(clientCfg)
}

func ensureDockerAvailable(ctx context.Context) error {
	if _, err := exec.LookPath("docker"); err != nil {
		if runningInCI() {
			return fmt.Errorf("docker is required for OpenFGA integration tests when GESTALT_TEST_OPENFGA_API_URL is not set")
		}
		return &skipBackendError{reason: "docker is required for OpenFGA integration tests when GESTALT_TEST_OPENFGA_API_URL is not set"}
	}

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if output, err := execCommand(checkCtx, "docker", "info"); err != nil {
		if runningInCI() {
			return fmt.Errorf("docker is unavailable for OpenFGA integration tests: %v (%s)", err, strings.TrimSpace(output))
		}
		return &skipBackendError{reason: fmt.Sprintf("docker is unavailable for OpenFGA integration tests: %v (%s)", err, strings.TrimSpace(output))}
	}
	return nil
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

func newSocketPath(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gst-authz-openfga-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}

func newUnixConn(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			conn, dialErr := grpc.NewClient(
				"passthrough:///"+socket,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", addr)
				}),
			)
			if dialErr != nil {
				t.Fatalf("grpc.NewClient: %v", dialErr)
			}
			conn.Connect()
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %q was not created", socket)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitServeResult(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not stop after context cancellation")
	}
}

func mustStruct(t *testing.T, fields map[string]any) *structpb.Struct {
	t.Helper()
	value, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	return value
}

func resourceIDs(resources []*proto.Resource) []string {
	out := make([]string, len(resources))
	for i, resource := range resources {
		out[i] = resource.GetId()
	}
	return out
}

func subjectIDs(subjects []*proto.Subject) []string {
	out := make([]string, len(subjects))
	for i, subject := range subjects {
		out[i] = subject.GetId()
	}
	return out
}

func actionNames(actions []*proto.Action) []string {
	out := make([]string, len(actions))
	for i, action := range actions {
		out[i] = action.GetName()
	}
	return out
}

func relationshipPairs(relationships []*proto.Relationship) []string {
	out := make([]string, len(relationships))
	for i, relationship := range relationships {
		out[i] = relationship.GetRelation() + ":" + relationship.GetResource().GetType() + "/" + relationship.GetResource().GetId()
	}
	return out
}

func roundTripModel() *proto.AuthorizationModel {
	return &proto.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*proto.AuthorizationModelResourceType{{
			Name: "document",
			Relations: []*proto.AuthorizationModelRelation{
				{Name: "viewer", SubjectTypes: []string{"user"}},
				{Name: "editor", SubjectTypes: []string{"user"}},
			},
			Actions: []*proto.AuthorizationModelAction{
				{Name: "read", Relations: []string{"viewer", "editor"}},
				{Name: "write", Relations: []string{"editor"}},
			},
		}},
	}
}

func expandedModel() *proto.AuthorizationModel {
	return &proto.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*proto.AuthorizationModelResourceType{{
			Name: "document",
			Relations: []*proto.AuthorizationModelRelation{
				{Name: "viewer", SubjectTypes: []string{"user"}},
				{Name: "editor", SubjectTypes: []string{"user"}},
				{Name: "owner", SubjectTypes: []string{"user"}},
			},
			Actions: []*proto.AuthorizationModelAction{
				{Name: "read", Relations: []string{"viewer", "editor", "owner"}},
				{Name: "write", Relations: []string{"editor", "owner"}},
				{Name: "admin", Relations: []string{"owner"}},
			},
		}},
	}
}

func rotatedModel() *proto.AuthorizationModel {
	return &proto.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*proto.AuthorizationModelResourceType{{
			Name: "document",
			Relations: []*proto.AuthorizationModelRelation{
				{Name: "viewer", SubjectTypes: []string{"user"}},
				{Name: "editor", SubjectTypes: []string{"group"}},
			},
			Actions: []*proto.AuthorizationModelAction{
				{Name: "read", Relations: []string{"viewer", "editor"}},
				{Name: "write", Relations: []string{"editor"}},
			},
		}},
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
