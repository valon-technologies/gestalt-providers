package modal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	modalclient "github.com/modal-labs/modal-client/go"
	modalproto "github.com/modal-labs/modal-client/go/proto/modal_proto"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRuntimeProviderContractPassesHostServiceEnv(t *testing.T) {
	t.Parallel()

	const hostServiceEnv = gestalt.EnvHostServiceSocket
	if !isHostServiceEnvVar(hostServiceEnv) {
		t.Fatalf("%s should be accepted as a relay host service env", hostServiceEnv)
	}
	env := buildPluginEnv(gestalt.StartHostedPluginRequest{
		Env: map[string]string{
			"CUSTOM":       "value",
			hostServiceEnv: "tls://host-service-relay.gestalt.example:443",
		},
	}, "tcp://0.0.0.0:50051")

	if got, want := env[hostServiceEnv], "tls://host-service-relay.gestalt.example:443"; got != want {
		t.Fatalf("host service socket env = %q, want %q", got, want)
	}
	if got, want := env[envProviderSocket], "tcp://0.0.0.0:50051"; got != want {
		t.Fatalf("provider socket env = %q, want %q", got, want)
	}
	if got, want := env["CUSTOM"], "value"; got != want {
		t.Fatalf("custom env = %q, want %q", got, want)
	}
}

func TestRuntimeProviderContractListsSessionsWithLifecycle(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, time.April, 27, 14, 0, 0, 0, time.UTC)
	recommendedDrainAt := startedAt.Add(4 * time.Minute)
	expiresAt := startedAt.Add(5 * time.Minute)
	provider := &Provider{
		name: "modal",
		sessions: map[string]*session{
			"session-1": {
				id:                 "session-1",
				state:              sessionStateRunning,
				metadata:           map[string]string{"provider_kind": "agent"},
				startedAt:          startedAt,
				recommendedDrainAt: &recommendedDrainAt,
				expiresAt:          &expiresAt,
				stateReason:        "exited",
				stateMessage:       "plugin process exited with status 137",
			},
		},
	}
	client := startRuntimeProviderServer(t, provider)

	sessions, err := client.ListSessions(context.Background())
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("ListSessions len = %d, want 1", len(sessions))
	}
	session := sessions[0]
	if got, want := session.ID, "session-1"; got != want {
		t.Fatalf("session id = %q, want %q", got, want)
	}
	assertTimePtrEqual(t, "started_at", session.Lifecycle.StartedAt, startedAt)
	assertTimePtrEqual(t, "recommended_drain_at", session.Lifecycle.RecommendedDrainAt, recommendedDrainAt)
	assertTimePtrEqual(t, "expires_at", session.Lifecycle.ExpiresAt, expiresAt)
	if got, want := session.StateReason, "exited"; got != want {
		t.Fatalf("state_reason = %q, want %q", got, want)
	}
	if got, want := session.StateMessage, "plugin process exited with status 137"; got != want {
		t.Fatalf("state_message = %q, want %q", got, want)
	}
}

func TestRuntimeProviderContractStartSessionHasNoLifecycleBeforeSandbox(t *testing.T) {
	t.Parallel()

	provider := New()
	provider.name = "modal"
	provider.client = &modalclient.Client{}
	provider.cfg = Config{App: "gestalt-test", Timeout: 5 * time.Minute}
	client := startRuntimeProviderServer(t, provider)

	session, err := client.StartSession(context.Background(), gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "agent",
		Image:      "python:3.14-slim-bookworm",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if hasLifecycle(session.Lifecycle) {
		t.Fatalf("StartSession lifecycle = %#v, want empty before Modal sandbox creation", session.Lifecycle)
	}
}

func TestRuntimeProviderContractResetSessionSandboxClearsLifecycle(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, time.April, 27, 14, 0, 0, 0, time.UTC)
	recommendedDrainAt := startedAt.Add(4 * time.Minute)
	expiresAt := startedAt.Add(5 * time.Minute)
	provider := &Provider{
		name: "modal",
		sessions: map[string]*session{
			"session-1": {
				id:                 "session-1",
				state:              sessionStateReady,
				startedAt:          startedAt,
				recommendedDrainAt: &recommendedDrainAt,
				expiresAt:          &expiresAt,
				sandbox:            &modalclient.Sandbox{},
			},
		},
	}

	provider.resetSessionSandbox("session-1", provider.sessions["session-1"].sandbox)

	cloned := cloneSession(provider.sessions["session-1"])
	if hasLifecycle(cloned.Lifecycle) {
		t.Fatalf("lifecycle after reset = %#v, want empty", cloned.Lifecycle)
	}
	if provider.sessions["session-1"].sandbox != nil {
		t.Fatal("sandbox after reset is still set")
	}
}

func TestRuntimeProviderContractRestoresTaggedModalSandboxAcrossProviders(t *testing.T) {
	t.Parallel()

	fakeModal := newFakeModalControlPlane()
	sessionID := "session-cross"
	fakeModal.addSandbox(&fakeModalSandbox{
		id:    "sb-restored",
		appID: fakeModal.appID,
		name:  sandboxName("agent-provider", sessionID),
		tags: modalSessionTags(sessionID, "modal", map[string]string{
			"provider_name": "agent-provider",
			"provider_kind": "agent",
		}),
	})
	provider := newFakeModalProvider(t, fakeModal)
	client := startRuntimeProviderServer(t, provider)

	session, err := client.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("GetSession restore: %v", err)
	}
	if got, want := session.ID, sessionID; got != want {
		t.Fatalf("restored session id = %q, want %q", got, want)
	}
	if got, want := session.State, sessionStateRunning; got != want {
		t.Fatalf("restored session state = %q, want %q", got, want)
	}
	if got, want := session.Metadata["provider_name"], "agent-provider"; got != want {
		t.Fatalf("restored provider_name = %q, want %q", got, want)
	}
	if got, want := session.Metadata["provider_kind"], "agent"; got != want {
		t.Fatalf("restored provider_kind = %q, want %q", got, want)
	}
	if got, want := session.StateReason, "restored"; got != want {
		t.Fatalf("restored state_reason = %q, want %q", got, want)
	}
	if !strings.Contains(session.StateMessage, "plugin process handle is unavailable") {
		t.Fatalf("restored state_message = %q, want process-handle limitation", session.StateMessage)
	}

	provider.mu.Lock()
	cached := provider.sessions[sessionID]
	provider.mu.Unlock()
	if cached == nil || !cached.restored {
		t.Fatalf("cached session = %#v, want restored session", cached)
	}

	exitCode := 0
	fakeModal.setPollCode("sb-restored", &exitCode)
	session, err = client.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("GetSession revalidate: %v", err)
	}
	if got, want := session.State, sessionStateStopped; got != want {
		t.Fatalf("revalidated session state = %q, want %q", got, want)
	}
	if got, want := session.StateReason, "exited"; got != want {
		t.Fatalf("revalidated state_reason = %q, want %q", got, want)
	}
}

func TestRuntimeProviderContractStopsRestoredModalSandboxAcrossProviders(t *testing.T) {
	t.Parallel()

	fakeModal := newFakeModalControlPlane()
	sessionID := "session-stop"
	fakeModal.addSandbox(&fakeModalSandbox{
		id:    "sb-stop",
		appID: fakeModal.appID,
		name:  sandboxName("agent-provider", sessionID),
		tags: modalSessionTags(sessionID, "modal", map[string]string{
			"provider_name": "agent-provider",
			"provider_kind": "agent",
		}),
	})
	provider := newFakeModalProvider(t, fakeModal)
	client := startRuntimeProviderServer(t, provider)

	if err := client.StopSession(context.Background(), sessionID); err != nil {
		t.Fatalf("StopSession: %v", err)
	}
	if !fakeModal.isTerminated("sb-stop") {
		t.Fatal("restored sandbox is still active after StopSession")
	}
}

func TestRuntimeProviderContractRestoredSessionCannotLaunch(t *testing.T) {
	t.Parallel()

	fakeModal := newFakeModalControlPlane()
	sessionID := "session-readonly"
	fakeModal.addSandbox(&fakeModalSandbox{
		id:    "sb-readonly",
		appID: fakeModal.appID,
		name:  sandboxName("agent-provider", sessionID),
		tags: modalSessionTags(sessionID, "modal", map[string]string{
			"provider_name": "agent-provider",
			"provider_kind": "agent",
		}),
	})
	provider := newFakeModalProvider(t, fakeModal)
	client := startRuntimeProviderServer(t, provider)

	if _, err := client.GetSession(context.Background(), sessionID); err != nil {
		t.Fatalf("GetSession restore: %v", err)
	}

	_, launchErr := client.StartPlugin(context.Background(), gestalt.StartHostedPluginRequest{
		SessionID:  sessionID,
		PluginName: "agent-provider",
		Command:    "/bin/true",
	})
	if status.Code(launchErr) != codes.FailedPrecondition {
		t.Fatalf("StartPlugin code = %v, want FailedPrecondition: %v", status.Code(launchErr), launchErr)
	}
}

func TestRuntimeProviderContractDuplicateTaggedModalSandboxesFailClosed(t *testing.T) {
	t.Parallel()

	fakeModal := newFakeModalControlPlane()
	sessionID := "session-duplicate"
	tags := modalSessionTags(sessionID, "modal", map[string]string{
		"provider_name": "agent-provider",
		"provider_kind": "agent",
	})
	fakeModal.addSandbox(&fakeModalSandbox{
		id:    "sb-duplicate-1",
		appID: fakeModal.appID,
		name:  sandboxName("agent-provider", sessionID),
		tags:  tags,
	})
	fakeModal.addSandbox(&fakeModalSandbox{
		id:    "sb-duplicate-2",
		appID: fakeModal.appID,
		name:  sandboxName("agent-provider", sessionID),
		tags:  tags,
	})
	provider := newFakeModalProvider(t, fakeModal)
	client := startRuntimeProviderServer(t, provider)

	_, err := client.GetSession(context.Background(), sessionID)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetSession code = %v, want FailedPrecondition: %v", status.Code(err), err)
	}
}

func TestRuntimeProviderContractRestoreTreatsSandboxGoneAfterListAsNotFound(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		configure func(*fakeModalSandbox)
	}{
		{
			name: "tags",
			configure: func(sandbox *fakeModalSandbox) {
				sandbox.missingOnTagsGet = true
			},
		},
		{
			name: "poll",
			configure: func(sandbox *fakeModalSandbox) {
				sandbox.missingOnWait = true
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fakeModal := newFakeModalControlPlane()
			sessionID := "session-gone-" + tc.name
			sandbox := &fakeModalSandbox{
				id:    "sb-gone-" + tc.name,
				appID: fakeModal.appID,
				name:  sandboxName("agent-provider", sessionID),
				tags: modalSessionTags(sessionID, "modal", map[string]string{
					"provider_name": "agent-provider",
					"provider_kind": "agent",
				}),
			}
			tc.configure(sandbox)
			fakeModal.addSandbox(sandbox)
			provider := newFakeModalProvider(t, fakeModal)
			client := startRuntimeProviderServer(t, provider)

			_, err := client.GetSession(context.Background(), sessionID)
			if status.Code(err) != codes.NotFound {
				t.Fatalf("GetSession code = %v, want NotFound: %v", status.Code(err), err)
			}
		})
	}
}

func TestRuntimeProviderContractStopRestoredIgnoresSandboxGoneAfterList(t *testing.T) {
	t.Parallel()

	fakeModal := newFakeModalControlPlane()
	sessionID := "session-stop-gone"
	fakeModal.addSandbox(&fakeModalSandbox{
		id:                 "sb-stop-gone",
		appID:              fakeModal.appID,
		name:               sandboxName("agent-provider", sessionID),
		missingOnTerminate: true,
		tags: modalSessionTags(sessionID, "modal", map[string]string{
			"provider_name": "agent-provider",
			"provider_kind": "agent",
		}),
	})
	provider := newFakeModalProvider(t, fakeModal)
	client := startRuntimeProviderServer(t, provider)

	if err := client.StopSession(context.Background(), sessionID); err != nil {
		t.Fatalf("StopSession: %v", err)
	}
}

func TestRuntimeProviderContractTagsModalSandboxBeforeTunnelLookup(t *testing.T) {
	t.Parallel()

	fakeModal := newFakeModalControlPlane()
	provider := newFakeModalProvider(t, fakeModal)
	sessionID := "session-tagged"
	provider.sessions[sessionID] = &session{
		id:       sessionID,
		state:    sessionStateReady,
		image:    "python:3.14-slim-bookworm",
		metadata: map[string]string{"provider_name": "agent-provider", "provider_kind": "agent"},
		bindings: map[string]string{},
	}
	req := gestalt.StartHostedPluginRequest{
		SessionID:  sessionID,
		PluginName: "agent-provider",
	}
	logs := newSessionLogSink(sessionID, &provider.sessions[sessionID].logSeq, nil)

	sandbox, tunnel, err := provider.ensureSessionSandbox(context.Background(), provider.client, provider.cfg, req, logs)
	if err != nil {
		t.Fatalf("ensureSessionSandbox: %v", err)
	}
	if sandbox == nil {
		t.Fatal("ensureSessionSandbox sandbox = nil")
	}
	if tunnel == nil {
		t.Fatal("ensureSessionSandbox tunnel = nil")
	}

	if secretRequests := fakeModal.secretRequestsSnapshot(); len(secretRequests) != 0 {
		t.Fatalf("SecretGetOrCreate requests = %d, want 0 for public image", len(secretRequests))
	}
	imageRequests := fakeModal.imageRequestsSnapshot()
	if len(imageRequests) != 1 {
		t.Fatalf("ImageGetOrCreate requests = %d, want 1", len(imageRequests))
	}
	imageRequest := imageRequests[0]
	if got, want := imageRequest.registryAuthType, modalproto.RegistryAuthType_REGISTRY_AUTH_TYPE_UNSPECIFIED; got != want {
		t.Fatalf("image registry auth type = %v, want %v for public image", got, want)
	}
	if imageRequest.registrySecretID != "" {
		t.Fatalf("image registry secret id = %q, want empty for public image", imageRequest.registrySecretID)
	}

	created := fakeModal.sandboxByID(sandbox.SandboxID)
	if created == nil {
		t.Fatalf("fake sandbox %q was not recorded", sandbox.SandboxID)
	}
	if got, want := created.name, "gestalt-session-tagged"; got != want {
		t.Fatalf("sandbox name = %q, want %q", got, want)
	}
	wantTags := modalSessionTags(sessionID, "modal", map[string]string{
		"provider_name": "agent-provider",
		"provider_kind": "agent",
	})
	for key, want := range wantTags {
		if got := created.tags[key]; got != want {
			t.Fatalf("sandbox tag %s = %q, want %q", key, got, want)
		}
	}
	events := fakeModal.eventsSnapshot()
	tagIndex := indexEvent(events, "tags-set:"+sandbox.SandboxID)
	tunnelIndex := indexEvent(events, "tunnels:"+sandbox.SandboxID)
	if tagIndex < 0 || tunnelIndex < 0 {
		t.Fatalf("events = %v, want tag and tunnel events", events)
	}
	if tagIndex > tunnelIndex {
		t.Fatalf("events = %v, want tags-set before tunnels", events)
	}
}

func TestRuntimeProviderContractUsesDockerConfigImagePullAuthForPrivateRegistry(t *testing.T) {
	t.Parallel()

	fakeModal := newFakeModalControlPlane()
	provider := newFakeModalProvider(t, fakeModal)
	client := startRuntimeProviderServer(t, provider)

	session, err := client.StartSession(context.Background(), gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "agent-provider",
		Image:      "ghcr.io/valon-technologies/agent-simple-runtime:latest",
		ImagePullAuth: &gestalt.PluginRuntimeImagePullAuth{
			DockerConfigJSON: `{"auths":{"ghcr.io":{"username":" ghcr-user ","password":" ghcr-token "}}}`,
		},
		Metadata: map[string]string{"provider_name": "agent-provider", "provider_kind": "agent"},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	var logSeq uint64
	sandbox, tunnel, err := provider.ensureSessionSandbox(context.Background(), provider.client, provider.cfg, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "agent-provider",
	}, newSessionLogSink(session.ID, &logSeq, nil))
	if err != nil {
		t.Fatalf("ensureSessionSandbox: %v", err)
	}
	if sandbox == nil {
		t.Fatal("ensureSessionSandbox sandbox = nil")
	}
	if tunnel == nil {
		t.Fatal("ensureSessionSandbox tunnel = nil")
	}

	secretRequests := fakeModal.secretRequestsSnapshot()
	if len(secretRequests) != 1 {
		t.Fatalf("SecretGetOrCreate requests = %d, want 1", len(secretRequests))
	}
	secretRequest := secretRequests[0]
	if got, want := secretRequest.env[registryUsernameEnv], "ghcr-user"; got != want {
		t.Fatalf("registry username secret value = %q, want %q", got, want)
	}
	if got, want := secretRequest.env[registryPasswordEnv], " ghcr-token "; got != want {
		t.Fatalf("registry password secret value = %q, want %q", got, want)
	}
	if got, want := secretRequest.environment, "test-env"; got != want {
		t.Fatalf("secret environment = %q, want %q", got, want)
	}
	if got, want := secretRequest.objectCreationType, modalproto.ObjectCreationType_OBJECT_CREATION_TYPE_EPHEMERAL; got != want {
		t.Fatalf("secret object creation type = %v, want %v", got, want)
	}

	imageRequests := fakeModal.imageRequestsSnapshot()
	if len(imageRequests) != 1 {
		t.Fatalf("ImageGetOrCreate requests = %d, want 1", len(imageRequests))
	}
	imageRequest := imageRequests[0]
	if got, want := imageRequest.registryAuthType, modalproto.RegistryAuthType_REGISTRY_AUTH_TYPE_STATIC_CREDS; got != want {
		t.Fatalf("image registry auth type = %v, want %v", got, want)
	}
	if got, want := imageRequest.registrySecretID, secretRequest.secretID; got != want {
		t.Fatalf("image registry secret id = %q, want %q", got, want)
	}
}

func TestRuntimeProviderContractUsesDockerConfigAuthFieldForPrivateRegistry(t *testing.T) {
	t.Parallel()

	fakeModal := newFakeModalControlPlane()
	provider := newFakeModalProvider(t, fakeModal)
	client := startRuntimeProviderServer(t, provider)

	session, err := client.StartSession(context.Background(), gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "agent-provider",
		Image:      "ghcr.io/valon-technologies/agent-simple-runtime:latest",
		ImagePullAuth: &gestalt.PluginRuntimeImagePullAuth{
			DockerConfigJSON: `{"auths":{"ghcr.io":{"auth":"Z2hjci11c2VyOmdoY3ItdG9rZW4="}}}`,
		},
		Metadata: map[string]string{"provider_name": "agent-provider", "provider_kind": "agent"},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	var logSeq uint64
	_, _, err = provider.ensureSessionSandbox(context.Background(), provider.client, provider.cfg, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "agent-provider",
	}, newSessionLogSink(session.ID, &logSeq, nil))
	if err != nil {
		t.Fatalf("ensureSessionSandbox: %v", err)
	}

	secretRequests := fakeModal.secretRequestsSnapshot()
	if len(secretRequests) != 1 {
		t.Fatalf("SecretGetOrCreate requests = %d, want 1", len(secretRequests))
	}
	if got, want := secretRequests[0].env[registryUsernameEnv], "ghcr-user"; got != want {
		t.Fatalf("registry username secret value = %q, want %q", got, want)
	}
	if got, want := secretRequests[0].env[registryPasswordEnv], "ghcr-token"; got != want {
		t.Fatalf("registry password secret value = %q, want %q", got, want)
	}
}

func TestRuntimeProviderContractUsesDockerHubImagePullAuth(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name             string
		image            string
		dockerConfigJSON string
	}{
		{
			name:             "single component tag",
			image:            "ubuntu:latest",
			dockerConfigJSON: `{"auths":{"https://index.docker.io/v1/":{"username":"docker-user","password":"docker-token"}}}`,
		},
		{
			name:             "namespace image tag",
			image:            "library/ubuntu:latest",
			dockerConfigJSON: `{"auths":{"registry-1.docker.io":{"username":"docker-user","password":"docker-token"}}}`,
		},
		{
			name:             "explicit docker host",
			image:            "docker.io/library/ubuntu:latest",
			dockerConfigJSON: `{"auths":{"docker.io":{"username":"docker-user","password":"docker-token"}}}`,
		},
		{
			name:             "single component digest",
			image:            "busybox@sha256:1111111111111111111111111111111111111111111111111111111111111111",
			dockerConfigJSON: `{"auths":{"docker.io":{"username":"docker-user","password":"docker-token"}}}`,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fakeModal := newFakeModalControlPlane()
			provider := newFakeModalProvider(t, fakeModal)
			client := startRuntimeProviderServer(t, provider)

			session, err := client.StartSession(context.Background(), gestalt.StartPluginRuntimeSessionRequest{
				PluginName: "agent-provider",
				Image:      tc.image,
				ImagePullAuth: &gestalt.PluginRuntimeImagePullAuth{
					DockerConfigJSON: tc.dockerConfigJSON,
				},
			})
			if err != nil {
				t.Fatalf("StartSession: %v", err)
			}

			var logSeq uint64
			_, _, err = provider.ensureSessionSandbox(context.Background(), provider.client, provider.cfg, gestalt.StartHostedPluginRequest{
				SessionID:  session.ID,
				PluginName: "agent-provider",
			}, newSessionLogSink(session.ID, &logSeq, nil))
			if err != nil {
				t.Fatalf("ensureSessionSandbox: %v", err)
			}

			secretRequests := fakeModal.secretRequestsSnapshot()
			if len(secretRequests) != 1 {
				t.Fatalf("SecretGetOrCreate requests = %d, want 1", len(secretRequests))
			}
			if got, want := secretRequests[0].env[registryUsernameEnv], "docker-user"; got != want {
				t.Fatalf("registry username secret value = %q, want %q", got, want)
			}
			if got, want := secretRequests[0].env[registryPasswordEnv], "docker-token"; got != want {
				t.Fatalf("registry password secret value = %q, want %q", got, want)
			}
		})
	}
}

func TestRuntimeProviderContractRejectsInvalidImagePullAuth(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		auth *gestalt.PluginRuntimeImagePullAuth
	}{
		{
			name: "blank Docker config JSON",
			auth: &gestalt.PluginRuntimeImagePullAuth{},
		},
		{
			name: "invalid Docker config JSON",
			auth: &gestalt.PluginRuntimeImagePullAuth{
				DockerConfigJSON: `{`,
			},
		},
		{
			name: "missing auths",
			auth: &gestalt.PluginRuntimeImagePullAuth{
				DockerConfigJSON: `{}`,
			},
		},
		{
			name: "missing registry entry",
			auth: &gestalt.PluginRuntimeImagePullAuth{
				DockerConfigJSON: `{"auths":{"index.docker.io/v1":{"username":"docker-user","password":"docker-token"}}}`,
			},
		},
		{
			name: "missing password",
			auth: &gestalt.PluginRuntimeImagePullAuth{
				DockerConfigJSON: `{"auths":{"ghcr.io":{"username":"ghcr-user"}}}`,
			},
		},
		{
			name: "identity token only",
			auth: &gestalt.PluginRuntimeImagePullAuth{
				DockerConfigJSON: `{"auths":{"ghcr.io":{"identitytoken":"token"}}}`,
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			provider := New()
			provider.name = "modal"
			provider.client = &modalclient.Client{}
			provider.cfg = Config{App: "gestalt-test", Timeout: 5 * time.Minute}
			client := startRuntimeProviderServer(t, provider)

			_, err := client.StartSession(context.Background(), gestalt.StartPluginRuntimeSessionRequest{
				PluginName:    "agent-provider",
				Image:         "ghcr.io/valon-technologies/agent-simple-runtime:latest",
				ImagePullAuth: tc.auth,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("StartSession code = %v, want InvalidArgument: %v", status.Code(err), err)
			}

			provider.mu.Lock()
			sessionCount := len(provider.sessions)
			provider.mu.Unlock()
			if sessionCount != 0 {
				t.Fatalf("provider sessions = %d, want 0", sessionCount)
			}
		})
	}
}

func TestRuntimeProviderContractRelayOnlyAgentHostLaunchSkipsHostnameProxy(t *testing.T) {
	t.Parallel()

	params, err := buildSandboxCreateParams(context.Background(), Config{}, gestalt.StartHostedPluginRequest{
		PluginName:   "agent-provider",
		AllowedHosts: []string{"host-service-relay.gestalt.example"},
		Env: map[string]string{
			gestalt.EnvHostServiceSocket: "tls://host-service-relay.gestalt.example:7443",
		},
	}, "session-1")
	if err != nil {
		t.Fatalf("buildSandboxCreateParams: %v", err)
	}
	if len(params.CIDRAllowlist) != 0 {
		t.Fatalf("CIDRAllowlist = %v, want none for relay-only agent host launch", params.CIDRAllowlist)
	}
}

func TestRuntimeProviderContractNonRelayAllowedHostStillRequiresProxy(t *testing.T) {
	t.Parallel()

	_, err := buildSandboxCreateParams(context.Background(), Config{}, gestalt.StartHostedPluginRequest{
		PluginName:   "agent-provider",
		AllowedHosts: []string{"api.github.com"},
		Env: map[string]string{
			gestalt.EnvHostServiceSocket: "tls://host-service-relay.gestalt.example:7443",
		},
	}, "session-1")
	if err == nil || !strings.Contains(err.Error(), "HTTP_PROXY or HTTPS_PROXY is required") {
		t.Fatalf("buildSandboxCreateParams error = %v, want missing proxy precondition", err)
	}
}

func TestNewProviderIDsAreBootUnique(t *testing.T) {
	t.Parallel()

	first := New().newID("session")
	second := New().newID("session")

	if first == second {
		t.Fatalf("first session id = %q, second = %q; want boot-unique ids", first, second)
	}
	if !strings.HasPrefix(first, "session-") {
		t.Fatalf("first session id = %q, want session- prefix", first)
	}
	if !strings.HasPrefix(second, "session-") {
		t.Fatalf("second session id = %q, want session- prefix", second)
	}
}

func hasLifecycle(lifecycle gestalt.PluginRuntimeSessionLifecycle) bool {
	return lifecycle.StartedAt != nil || lifecycle.RecommendedDrainAt != nil || lifecycle.ExpiresAt != nil
}

func assertTimePtrEqual(t *testing.T, name string, got *time.Time, want time.Time) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = nil, want %s", name, want)
	}
	if !got.Equal(want) {
		t.Fatalf("%s = %s, want %s", name, got, want)
	}
}

func startRuntimeProviderServer(t *testing.T, provider *Provider) *Provider {
	t.Helper()
	return provider
}

type fakeModalControlPlane struct {
	modalproto.UnimplementedModalClientServer

	mu            sync.Mutex
	appID         string
	nextSandboxID int
	nextSecretID  int
	sandboxes     map[string]*fakeModalSandbox
	secrets       []fakeSecretRequest
	images        []fakeImageRequest
	events        []string
}

type fakeSecretRequest struct {
	secretID           string
	environment        string
	objectCreationType modalproto.ObjectCreationType
	env                map[string]string
}

type fakeImageRequest struct {
	registryAuthType modalproto.RegistryAuthType
	registrySecretID string
	dockerfile       []string
}

type fakeModalSandbox struct {
	id         string
	appID      string
	name       string
	tags       map[string]string
	terminated bool
	pollCode   *int
	createdAt  float64

	missingOnTagsGet   bool
	missingOnWait      bool
	missingOnTerminate bool
}

func newFakeModalControlPlane() *fakeModalControlPlane {
	return &fakeModalControlPlane{
		appID:     "app-test",
		sandboxes: map[string]*fakeModalSandbox{},
	}
}

func newFakeModalProvider(t *testing.T, fakeModal *fakeModalControlPlane) *Provider {
	t.Helper()
	client := startFakeModalClient(t, fakeModal)
	provider := New()
	provider.name = "modal"
	provider.client = client
	provider.cfg = Config{
		App:         "gestalt-test",
		Environment: "test-env",
		Timeout:     5 * time.Minute,
	}
	provider.sessions = map[string]*session{}
	return provider
}

func startFakeModalClient(t *testing.T, fakeModal *fakeModalControlPlane) *modalclient.Client {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	modalproto.RegisterModalClientServer(server, fakeModal)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("fake modal server stopped: %v", err)
		}
	}()

	conn, err := grpc.NewClient(
		"passthrough:///modal-control-plane",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connect fake modal server: %v", err)
	}
	client, err := modalclient.NewClientWithOptions(&modalclient.ClientParams{
		TokenID:            "test-token-id",
		TokenSecret:        "test-token-secret",
		Environment:        "test-env",
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		ControlPlaneClient: modalproto.NewModalClientClient(conn),
		ControlPlaneConn:   conn,
	})
	if err != nil {
		t.Fatalf("create fake modal client: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
		server.Stop()
		_ = listener.Close()
	})
	return client
}

func (f *fakeModalControlPlane) AppGetOrCreate(ctx context.Context, req *modalproto.AppGetOrCreateRequest) (*modalproto.AppGetOrCreateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.appID == "" {
		if req.GetObjectCreationType() != modalproto.ObjectCreationType_OBJECT_CREATION_TYPE_CREATE_IF_MISSING {
			return nil, status.Error(codes.NotFound, "app not found")
		}
		f.appID = "app-test"
	}
	return modalproto.AppGetOrCreateResponse_builder{AppId: f.appID}.Build(), nil
}

func (f *fakeModalControlPlane) SecretGetOrCreate(ctx context.Context, req *modalproto.SecretGetOrCreateRequest) (*modalproto.SecretGetOrCreateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSecretID++
	secretID := fmt.Sprintf("secret-%d", f.nextSecretID)
	f.secrets = append(f.secrets, fakeSecretRequest{
		secretID:           secretID,
		environment:        req.GetEnvironmentName(),
		objectCreationType: req.GetObjectCreationType(),
		env:                cloneStringMap(req.GetEnvDict()),
	})
	return modalproto.SecretGetOrCreateResponse_builder{SecretId: secretID}.Build(), nil
}

func (f *fakeModalControlPlane) ImageGetOrCreate(ctx context.Context, req *modalproto.ImageGetOrCreateRequest) (*modalproto.ImageGetOrCreateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	imageRequest := fakeImageRequest{}
	if image := req.GetImage(); image != nil {
		imageRequest.dockerfile = append([]string(nil), image.GetDockerfileCommands()...)
		if registryConfig := image.GetImageRegistryConfig(); registryConfig != nil {
			imageRequest.registryAuthType = registryConfig.GetRegistryAuthType()
			imageRequest.registrySecretID = registryConfig.GetSecretId()
		}
	}
	f.images = append(f.images, imageRequest)
	return modalproto.ImageGetOrCreateResponse_builder{
		ImageId: "image-test",
		Result: modalproto.GenericResult_builder{
			Status: modalproto.GenericResult_GENERIC_STATUS_SUCCESS,
		}.Build(),
	}.Build(), nil
}

func (f *fakeModalControlPlane) SandboxCreate(ctx context.Context, req *modalproto.SandboxCreateRequest) (*modalproto.SandboxCreateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSandboxID++
	sandboxID := fmt.Sprintf("sb-created-%d", f.nextSandboxID)
	name := ""
	if req.GetDefinition() != nil {
		name = req.GetDefinition().GetName()
	}
	f.sandboxes[sandboxID] = &fakeModalSandbox{
		id:        sandboxID,
		appID:     req.GetAppId(),
		name:      name,
		tags:      map[string]string{},
		createdAt: float64(f.nextSandboxID),
	}
	f.events = append(f.events, "create:"+sandboxID)
	return modalproto.SandboxCreateResponse_builder{SandboxId: sandboxID}.Build(), nil
}

func (f *fakeModalControlPlane) SandboxTagsSet(ctx context.Context, req *modalproto.SandboxTagsSetRequest) (*emptypb.Empty, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	sandbox := f.sandboxes[req.GetSandboxId()]
	if sandbox == nil {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	sandbox.tags = modalTagsToMap(req.GetTags())
	f.events = append(f.events, "tags-set:"+sandbox.id)
	return &emptypb.Empty{}, nil
}

func (f *fakeModalControlPlane) SandboxGetTunnels(ctx context.Context, req *modalproto.SandboxGetTunnelsRequest) (*modalproto.SandboxGetTunnelsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sandboxes[req.GetSandboxId()] == nil {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	f.events = append(f.events, "tunnels:"+req.GetSandboxId())
	return modalproto.SandboxGetTunnelsResponse_builder{
		Tunnels: []*modalproto.TunnelData{
			modalproto.TunnelData_builder{
				Host:          "plugin.example.test",
				Port:          443,
				ContainerPort: uint32(pluginGRPCPort),
			}.Build(),
		},
	}.Build(), nil
}

func (f *fakeModalControlPlane) SandboxList(ctx context.Context, req *modalproto.SandboxListRequest) (*modalproto.SandboxListResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.GetBeforeTimestamp() != 0 {
		return modalproto.SandboxListResponse_builder{}.Build(), nil
	}
	wantTags := modalTagsToMap(req.GetTags())
	var sandboxes []*modalproto.SandboxInfo
	for _, sandbox := range f.sandboxes {
		if sandbox == nil || sandbox.terminated {
			continue
		}
		if req.GetAppId() != "" && sandbox.appID != req.GetAppId() {
			continue
		}
		if !modalSessionTagsMatch(sandbox.tags, wantTags) {
			continue
		}
		sandboxes = append(sandboxes, modalproto.SandboxInfo_builder{
			Id:        sandbox.id,
			AppId:     sandbox.appID,
			Name:      sandbox.name,
			Tags:      mapToModalTags(sandbox.tags),
			CreatedAt: sandbox.createdAt,
		}.Build())
	}
	return modalproto.SandboxListResponse_builder{Sandboxes: sandboxes}.Build(), nil
}

func (f *fakeModalControlPlane) SandboxTagsGet(ctx context.Context, req *modalproto.SandboxTagsGetRequest) (*modalproto.SandboxTagsGetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	sandbox := f.sandboxes[req.GetSandboxId()]
	if sandbox == nil {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	if sandbox.missingOnTagsGet {
		sandbox.terminated = true
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	return modalproto.SandboxTagsGetResponse_builder{Tags: mapToModalTags(sandbox.tags)}.Build(), nil
}

func (f *fakeModalControlPlane) SandboxWait(ctx context.Context, req *modalproto.SandboxWaitRequest) (*modalproto.SandboxWaitResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	sandbox := f.sandboxes[req.GetSandboxId()]
	if sandbox == nil {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	if sandbox.missingOnWait {
		sandbox.terminated = true
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	if sandbox.pollCode == nil && !sandbox.terminated {
		return modalproto.SandboxWaitResponse_builder{}.Build(), nil
	}
	exitCode := int32(137)
	statusCode := modalproto.GenericResult_GENERIC_STATUS_TERMINATED
	if sandbox.pollCode != nil {
		exitCode = int32(*sandbox.pollCode)
		statusCode = modalproto.GenericResult_GENERIC_STATUS_SUCCESS
	}
	return modalproto.SandboxWaitResponse_builder{
		Result: modalproto.GenericResult_builder{
			Status:   statusCode,
			Exitcode: exitCode,
		}.Build(),
	}.Build(), nil
}

func (f *fakeModalControlPlane) SandboxTerminate(ctx context.Context, req *modalproto.SandboxTerminateRequest) (*modalproto.SandboxTerminateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	sandbox := f.sandboxes[req.GetSandboxId()]
	if sandbox == nil {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	if sandbox.missingOnTerminate {
		sandbox.terminated = true
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	sandbox.terminated = true
	f.events = append(f.events, "terminate:"+sandbox.id)
	return modalproto.SandboxTerminateResponse_builder{}.Build(), nil
}

func (f *fakeModalControlPlane) addSandbox(sandbox *fakeModalSandbox) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if sandbox.appID == "" {
		sandbox.appID = f.appID
	}
	if sandbox.tags == nil {
		sandbox.tags = map[string]string{}
	}
	if sandbox.createdAt == 0 {
		f.nextSandboxID++
		sandbox.createdAt = float64(f.nextSandboxID)
	}
	f.sandboxes[sandbox.id] = cloneFakeModalSandbox(sandbox)
}

func (f *fakeModalControlPlane) setPollCode(sandboxID string, code *int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if sandbox := f.sandboxes[sandboxID]; sandbox != nil {
		sandbox.pollCode = code
	}
}

func (f *fakeModalControlPlane) isTerminated(sandboxID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	sandbox := f.sandboxes[sandboxID]
	return sandbox != nil && sandbox.terminated
}

func (f *fakeModalControlPlane) sandboxByID(sandboxID string) *fakeModalSandbox {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneFakeModalSandbox(f.sandboxes[sandboxID])
}

func (f *fakeModalControlPlane) eventsSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

func (f *fakeModalControlPlane) secretRequestsSnapshot() []fakeSecretRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeSecretRequest, len(f.secrets))
	for i, req := range f.secrets {
		out[i] = req
		out[i].env = cloneStringMap(req.env)
	}
	return out
}

func (f *fakeModalControlPlane) imageRequestsSnapshot() []fakeImageRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeImageRequest, len(f.images))
	for i, req := range f.images {
		out[i] = req
		out[i].dockerfile = append([]string(nil), req.dockerfile...)
	}
	return out
}

func cloneFakeModalSandbox(sandbox *fakeModalSandbox) *fakeModalSandbox {
	if sandbox == nil {
		return nil
	}
	cloned := *sandbox
	cloned.tags = cloneStringMap(sandbox.tags)
	return &cloned
}

func mapToModalTags(tags map[string]string) []*modalproto.SandboxTag {
	out := make([]*modalproto.SandboxTag, 0, len(tags))
	for key, value := range tags {
		out = append(out, modalproto.SandboxTag_builder{
			TagName:  key,
			TagValue: value,
		}.Build())
	}
	return out
}

func modalTagsToMap(tags []*modalproto.SandboxTag) map[string]string {
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		if tag == nil {
			continue
		}
		out[tag.GetTagName()] = tag.GetTagValue()
	}
	return out
}

func indexEvent(events []string, event string) int {
	for i, got := range events {
		if got == event {
			return i
		}
	}
	return -1
}
