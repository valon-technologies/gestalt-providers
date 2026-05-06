package kubernetes

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProviderContractAdvertisesHostnameEgressOnlyWhenEnabled(t *testing.T) {
	provider := New()
	provider.cfg = testConfig()
	provider.runtime = &fakeRuntime{}

	support, err := provider.GetSupport(context.Background())
	if err != nil {
		t.Fatalf("GetSupport disabled: %v", err)
	}
	if support.EgressMode != gestalt.PluginRuntimeEgressModeNone {
		t.Fatalf("disabled EgressMode = %q, want none", support.EgressMode)
	}

	provider.cfg.HostnameEgress.Mode = hostnameEgressModePublicProxy
	support, err = provider.GetSupport(context.Background())
	if err != nil {
		t.Fatalf("GetSupport publicProxy: %v", err)
	}
	if support.EgressMode != gestalt.PluginRuntimeEgressModeHostname {
		t.Fatalf("publicProxy EgressMode = %q, want hostname", support.EgressMode)
	}
}

func TestProviderContractPassesImagePullAuthToRuntime(t *testing.T) {
	runtime := &fakeRuntime{}
	provider := New()
	provider.cfg = testConfig()
	provider.runtime = runtime

	_, err := provider.StartSession(context.Background(), gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "github",
		Image:      "registry.example/runtime:latest",
		ImagePullAuth: &gestalt.PluginRuntimeImagePullAuth{
			DockerConfigJSON: `{"auths":{"registry.example":{"username":"u","password":"p"}}}`,
		},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if got, want := runtime.startReq.DockerConfigJSON, `{"auths":{"registry.example":{"username":"u","password":"p"}}}`; got != want {
		t.Fatalf("DockerConfigJSON = %q, want %q", got, want)
	}
}

func TestProviderContractBuildsPluginEnv(t *testing.T) {
	env := buildPluginEnv(gestalt.StartHostedPluginRequest{
		PluginName: "claude",
		Env:        map[string]string{"CUSTOM": "value"},
	}, "/tmp/gestalt/plugin.sock")
	if got, want := env[envProviderSocket], "/tmp/gestalt/plugin.sock"; got != want {
		t.Fatalf("%s = %q, want %q", envProviderSocket, got, want)
	}
	if got, want := env[envProviderName], "claude"; got != want {
		t.Fatalf("%s = %q, want %q", envProviderName, got, want)
	}
	if got, want := env["CUSTOM"], "value"; got != want {
		t.Fatalf("CUSTOM = %q, want %q", got, want)
	}
}

func TestProviderContractStartedPluginHealthFailureMapsFailed(t *testing.T) {
	runtime := &fakeRuntime{execErr: errors.New("plugin exited")}
	session := runtimeSession{
		ID:            "session-1",
		PluginStarted: true,
		Handle: runtimeHandle{
			Name:      "session-1",
			Namespace: "runtime-system",
			PodName:   "session-1",
			Ready:     true,
		},
	}

	if got, want := sessionStateForRuntime(context.Background(), runtime, session), sessionStateFailed; got != want {
		t.Fatalf("session state = %q, want %q", got, want)
	}
}

func TestProviderContractStartedPluginHealthWinsOverPendingReadiness(t *testing.T) {
	runtime := &fakeRuntime{}
	session := runtimeSession{
		ID:            "session-1",
		PluginStarted: true,
		Handle: runtimeHandle{
			Name:      "session-1",
			Namespace: "runtime-system",
			PodName:   "session-1",
			Ready:     false,
		},
	}

	if got, want := sessionStateForRuntime(context.Background(), runtime, session), sessionStateRunning; got != want {
		t.Fatalf("session state = %q, want %q", got, want)
	}
}

func TestProviderContractStartPluginBlocksFailedStartedSessionBeforeLaunch(t *testing.T) {
	runtime := &fakeRuntime{
		execErr: errors.New("plugin exited"),
		resolveSession: runtimeSession{
			ID:            "session-1",
			PluginStarted: true,
			Handle: runtimeHandle{
				Name:      "session-1",
				Namespace: "runtime-system",
				PodName:   "session-1",
				Ready:     true,
			},
		},
	}
	provider := New()
	provider.cfg = testConfig()
	provider.runtime = runtime

	_, err := provider.StartPlugin(context.Background(), gestalt.StartHostedPluginRequest{
		SessionID:  "session-1",
		PluginName: "github",
		Command:    "gestalt-plugin-github",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartPlugin error = %v, want FailedPrecondition", err)
	}
	if got, want := len(runtime.execCommands), 1; got != want {
		t.Fatalf("Exec calls = %d, want only health check", got)
	}
}

func TestProviderContractStartPluginBlocksStartedNotReadySessionBeforeLaunch(t *testing.T) {
	runtime := &fakeRuntime{
		resolveSession: runtimeSession{
			ID:            "session-1",
			PluginStarted: true,
			Handle: runtimeHandle{
				Name:      "session-1",
				Namespace: "runtime-system",
				PodName:   "session-1",
				Ready:     false,
			},
		},
	}
	provider := New()
	provider.cfg = testConfig()
	provider.runtime = runtime

	_, err := provider.StartPlugin(context.Background(), gestalt.StartHostedPluginRequest{
		SessionID:  "session-1",
		PluginName: "github",
		Command:    "gestalt-plugin-github",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartPlugin error = %v, want FailedPrecondition", err)
	}
	if got, want := len(runtime.execCommands), 1; got != want {
		t.Fatalf("Exec calls = %d, want only health check", got)
	}
}

type fakeRuntime struct {
	startReq       startRuntimeSessionRequest
	resolveSession runtimeSession
	execErr        error
	execCommands   [][]string
}

func (f *fakeRuntime) HealthCheck(context.Context) error { return nil }

func (f *fakeRuntime) Start(_ context.Context, req startRuntimeSessionRequest) (runtimeSession, error) {
	f.startReq = req
	return runtimeSession{
		ID:         req.Name,
		PluginName: req.PluginName,
		Metadata:   cloneStringMap(req.Metadata),
		Handle: runtimeHandle{
			Name:      req.Name,
			Namespace: req.Namespace,
			PodName:   req.Name,
			Ready:     true,
		},
	}, nil
}

func (f *fakeRuntime) ResolveSession(_ context.Context, namespace, sessionID string) (runtimeSession, error) {
	if f.resolveSession.ID != "" {
		session := f.resolveSession
		if session.Handle.Name == "" {
			session.Handle.Name = session.ID
		}
		if session.Handle.Namespace == "" {
			session.Handle.Namespace = namespace
		}
		if session.Handle.PodName == "" {
			session.Handle.PodName = session.ID
		}
		return session, nil
	}
	return runtimeSession{
		ID: sessionID,
		Handle: runtimeHandle{
			Name:      sessionID,
			Namespace: namespace,
			PodName:   sessionID,
			Ready:     true,
		},
	}, nil
}

func (f *fakeRuntime) ListSessions(context.Context, string) ([]runtimeSession, error) {
	return nil, nil
}

func (f *fakeRuntime) Stop(context.Context, runtimeHandle) error { return nil }

func (f *fakeRuntime) Exec(_ context.Context, _ runtimeHandle, command []string, _ io.Reader) error {
	f.execCommands = append(f.execCommands, append([]string(nil), command...))
	return f.execErr
}

func (f *fakeRuntime) ForwardPort(context.Context, runtimeHandle, int) (tunnel, error) {
	return staticTunnel{target: "tcp://127.0.0.1:50051"}, nil
}

func (f *fakeRuntime) PodIPDialTarget(context.Context, runtimeHandle, int) (tunnel, error) {
	return staticTunnel{target: "tcp://10.20.30.40:50051"}, nil
}

func (f *fakeRuntime) ServiceDNSDialTarget(context.Context, runtimeHandle, int) (tunnel, error) {
	return staticTunnel{target: "tcp://session.runtime-system.svc.cluster.local:50051"}, nil
}

func (f *fakeRuntime) EnsureHostnameEgressPolicy(context.Context, runtimeHandle, hostnameEgressConfig) (string, error) {
	return "", nil
}

func (f *fakeRuntime) DeleteHostnameEgressPolicy(context.Context, runtimeHandle, string) error {
	return nil
}

func (f *fakeRuntime) AcquirePluginStartLease(context.Context, runtimeHandle, string, time.Duration) error {
	return nil
}

func (f *fakeRuntime) ReleasePluginStartLease(context.Context, runtimeHandle, string) error {
	return nil
}

func (f *fakeRuntime) MarkPluginStarted(context.Context, runtimeHandle, string, string) error {
	return nil
}

func (f *fakeRuntime) Close() error { return nil }
