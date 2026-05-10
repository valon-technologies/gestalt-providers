package gkeagentsandbox

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"golang.org/x/oauth2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	agentfake "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
	extfake "sigs.k8s.io/agent-sandbox/clients/k8s/extensions/clientset/versioned/fake"
	extv1alpha1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"
)

func TestRuntimeContractBuildsADCBackedGKERestConfig(t *testing.T) {
	t.Parallel()

	caData := []byte("test-ca")
	cfg := GKEConfig{
		ProjectID: "gitlab-peach-street",
		Location:  "us-east4",
		Cluster:   "gestalt-agent-sandbox",
		Endpoint:  gkeEndpointPrivate,
	}
	restConfig, err := gkeRESTConfigFromCluster(cfg, &containerpb.Cluster{
		Endpoint: "34.11.22.90",
		PrivateClusterConfig: &containerpb.PrivateClusterConfig{
			PrivateEndpoint: "172.24.192.2",
		},
		MasterAuth: &containerpb.MasterAuth{
			ClusterCaCertificate: base64.StdEncoding.EncodeToString(caData),
		},
	}, oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: "test-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}))
	if err != nil {
		t.Fatalf("gkeRESTConfigFromCluster: %v", err)
	}
	if got, want := restConfig.Host, "https://172.24.192.2"; got != want {
		t.Fatalf("Host = %q, want %q", got, want)
	}
	if got, want := string(restConfig.TLSClientConfig.CAData), string(caData); got != want {
		t.Fatalf("CAData = %q, want %q", got, want)
	}
	if restConfig.BearerToken != "" {
		t.Fatalf("BearerToken = %q, want empty token managed by WrapTransport", restConfig.BearerToken)
	}
	if restConfig.WrapTransport == nil {
		t.Fatal("WrapTransport = nil, want ADC OAuth transport")
	}

	var gotAuthorization string
	transport := restConfig.WrapTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		gotAuthorization = req.Header.Get("Authorization")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	}))
	req, err := http.NewRequest(http.MethodGet, restConfig.Host+"/version", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer resp.Body.Close()
	if got, want := gotAuthorization, "Bearer test-token"; got != want {
		t.Fatalf("Authorization = %q, want %q", got, want)
	}
}

func TestRuntimeContractHostnameEgressPolicyRestrictsDNSAndProxyTargets(t *testing.T) {
	t.Parallel()

	runtime := &kubernetesSandboxRuntime{
		core:       k8sfake.NewSimpleClientset(),
		agents:     agentfake.NewSimpleClientset(),
		extensions: extfake.NewSimpleClientset(),
		lookupIP: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("203.0.113.10")}}, nil
		},
		readFile: func(context.Context, sandboxHandle, string) (string, error) {
			return "search svc.cluster.local\nnameserver 10.96.0.10\nnameserver 169.254.20.10\n", nil
		},
	}

	name, err := runtime.EnsureHostnameEgressPolicy(context.Background(), sandboxHandle{
		Name:      "gestalt-agent-provider-session-1",
		Namespace: "runtime-system",
		PodName:   "sandbox-pod",
	}, hostnameEgressConfig{
		Endpoints: []hostnameEgressEndpoint{{
			Host: "proxy.gestalt.example",
			Port: 9443,
		}},
	})
	if err != nil {
		t.Fatalf("EnsureHostnameEgressPolicy: %v", err)
	}
	if got, want := name, "gestalt-agent-provider-session-1-egress"; got != want {
		t.Fatalf("policy name = %q, want %q", got, want)
	}

	policy, err := runtime.core.NetworkingV1().NetworkPolicies("runtime-system").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get NetworkPolicy: %v", err)
	}
	if got, want := policy.Spec.PodSelector.MatchLabels[runtimeSessionLabel], "gestalt-agent-provider-session-1"; got != want {
		t.Fatalf("pod selector runtime session = %q, want %q", got, want)
	}
	if len(policy.Spec.Egress) != 3 {
		t.Fatalf("egress rule count = %d, want 3", len(policy.Spec.Egress))
	}

	assertPolicyRule(t, policy.Spec.Egress[0], []string{"10.96.0.10/32", "169.254.20.10/32"}, 53, corev1.ProtocolUDP)
	assertPolicyRule(t, policy.Spec.Egress[1], []string{"10.96.0.10/32", "169.254.20.10/32"}, 53, corev1.ProtocolTCP)
	assertPolicyRule(t, policy.Spec.Egress[2], []string{"203.0.113.10/32"}, 9443, corev1.ProtocolTCP)

	longName, err := runtime.EnsureHostnameEgressPolicy(context.Background(), sandboxHandle{
		Name:      "gestalt-" + strings.Repeat("x", 55),
		Namespace: "runtime-system",
		PodName:   "sandbox-pod",
	}, hostnameEgressConfig{
		Endpoints: []hostnameEgressEndpoint{{
			Host: "proxy.gestalt.example",
			Port: 9443,
		}},
	})
	if err != nil {
		t.Fatalf("EnsureHostnameEgressPolicy long name: %v", err)
	}
	if len(longName) > 63 {
		t.Fatalf("long policy name length = %d, want <= 63: %q", len(longName), longName)
	}
	if !strings.HasSuffix(longName, "-egress") {
		t.Fatalf("long policy name = %q, want -egress suffix", longName)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func seedReadyClaimSandbox(ctx context.Context, runtime *kubernetesSandboxRuntime, namespace, claimName, sandboxName, podName, image string) error {
	var claim *extv1alpha1.SandboxClaim
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		claim, err = runtime.extensions.ExtensionsV1alpha1().SandboxClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
		if err == nil {
			break
		}
		if !k8serrors.IsNotFound(err) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	if claim == nil {
		return errors.New("SandboxClaim was not created")
	}
	_, err := runtime.agents.AgentsV1alpha1().Sandboxes(namespace).Create(ctx, &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: namespace,
			Annotations: map[string]string{
				sandboxv1alpha1.SandboxPodNameAnnotation: podName,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: extv1alpha1.SchemeGroupVersion.String(),
				Kind:       "SandboxClaim",
				Name:       claim.Name,
				UID:        claim.UID,
			}},
		},
		Status: sandboxv1alpha1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1alpha1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			}},
		},
	}, metav1.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}
	_, err = runtime.core.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  runtime.cfg.Container,
			Image: image,
		}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name:    runtime.cfg.Container,
			ImageID: "containerd://" + image,
		}}},
	}, metav1.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}
	claim.Status.SandboxStatus.Name = sandboxName
	_, err = runtime.extensions.ExtensionsV1alpha1().SandboxClaims(namespace).Update(ctx, claim, metav1.UpdateOptions{})
	return err
}

func TestRuntimeLabelsAndAnnotationsIncludeTenantMetadata(t *testing.T) {
	metadata := map[string]string{
		sessionTenantIDMetadataKey:   "acme",
		sessionTenantHostMetadataKey: "acme.dev.valon.tools",
	}

	labels := runtimeLabels("github", metadata)
	if got, want := labels[runtimeTenantLabel], "acme"; got != want {
		t.Fatalf("tenant label = %q, want %q", got, want)
	}
	annotations := runtimeAnnotations(metadata)
	if got, want := annotations[runtimeTenantAnnotation], "acme"; got != want {
		t.Fatalf("tenant annotation = %q, want %q", got, want)
	}
	if got, want := annotations[runtimeTenantHostAnnotation], "acme.dev.valon.tools"; got != want {
		t.Fatalf("tenant host annotation = %q, want %q", got, want)
	}
}

func TestRuntimeContractHostnameEgressPolicyRejectsManagedTemplates(t *testing.T) {
	t.Parallel()

	runtime := &kubernetesSandboxRuntime{
		core:   k8sfake.NewSimpleClientset(),
		agents: agentfake.NewSimpleClientset(),
		extensions: extfake.NewSimpleClientset(
			&extv1alpha1.SandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "python-runtime",
					Namespace: "runtime-system",
				},
				Spec: extv1alpha1.SandboxTemplateSpec{
					NetworkPolicyManagement: extv1alpha1.NetworkPolicyManagementManaged,
				},
			},
			&extv1alpha1.SandboxClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claim-1",
					Namespace: "runtime-system",
					UID:       "claim-uid-1",
				},
			},
		),
	}

	_, err := runtime.EnsureHostnameEgressPolicy(context.Background(), sandboxHandle{
		Name:      "gestalt-agent-provider-session-2",
		Namespace: "runtime-system",
		Mode:      "claim",
		ClaimName: "claim-1",
	}, hostnameEgressConfig{
		Template: "python-runtime",
		Endpoints: []hostnameEgressEndpoint{{
			Host: "proxy.gestalt.example",
			Port: 9443,
		}},
	})
	if err == nil {
		t.Fatal("EnsureHostnameEgressPolicy error = nil, want precondition failure")
	}
	var preconditionErr *hostnameEgressPreconditionError
	if !errors.As(err, &preconditionErr) {
		t.Fatalf("EnsureHostnameEgressPolicy error = %v, want hostname egress precondition", err)
	}

	policies, listErr := runtime.core.NetworkingV1().NetworkPolicies("runtime-system").List(context.Background(), metav1.ListOptions{})
	if listErr != nil {
		t.Fatalf("List NetworkPolicies: %v", listErr)
	}
	if got := len(policies.Items); got != 0 {
		t.Fatalf("network policy count = %d, want 0", got)
	}
}

func TestRuntimeContractStartClaimStampsFreshLifecycleAndRuntimeMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	runtime := &kubernetesSandboxRuntime{
		cfg: Config{
			Namespace:           "runtime-system",
			Container:           "runtime",
			SandboxReadyTimeout: 2 * time.Second,
			CleanupTimeout:      2 * time.Second,
			SessionTTL:          2 * time.Hour,
			SessionDrainBefore:  5 * time.Minute,
			WarmPool:            "none",
		},
		core:   k8sfake.NewSimpleClientset(),
		agents: agentfake.NewSimpleClientset(),
		extensions: extfake.NewSimpleClientset(&extv1alpha1.SandboxTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "agent-runtime",
				Namespace: "runtime-system",
			},
			Spec: extv1alpha1.SandboxTemplateSpec{
				PodTemplate: sandboxv1alpha1.PodTemplate{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:  "runtime",
						Image: "us-docker.pkg.dev/test/agent-runtime@sha256:current",
					}}},
				},
			},
		}),
	}
	readyErr := make(chan error, 1)
	go func() {
		readyErr <- seedReadyClaimSandbox(ctx, runtime, "runtime-system", "session-1", "session-1-sandbox", "session-1-pod", "us-docker.pkg.dev/test/agent-runtime@sha256:current")
	}()

	session, err := runtime.Start(ctx, startSandboxRequest{
		Name:       "session-1",
		PluginName: "agent-provider",
		Namespace:  "runtime-system",
		Template:   "agent-runtime",
		Metadata:   map[string]string{"tenant": "dev"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := <-readyErr; err != nil {
		t.Fatalf("seed ready claim: %v", err)
	}

	claim, err := runtime.extensions.ExtensionsV1alpha1().SandboxClaims("runtime-system").Get(ctx, "session-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get SandboxClaim: %v", err)
	}
	if claim.Spec.WarmPool == nil || *claim.Spec.WarmPool != extv1alpha1.WarmPoolPolicyNone {
		t.Fatalf("WarmPool = %v, want none", claim.Spec.WarmPool)
	}
	if claim.Spec.Lifecycle == nil || claim.Spec.Lifecycle.ShutdownTime == nil {
		t.Fatalf("Lifecycle.ShutdownTime missing")
	}
	if got, want := claim.Spec.Lifecycle.ShutdownPolicy, extv1alpha1.ShutdownPolicyDeleteForeground; got != want {
		t.Fatalf("Lifecycle.ShutdownPolicy = %q, want %q", got, want)
	}
	if got, want := claim.Annotations[metadataExpectedImage], "us-docker.pkg.dev/test/agent-runtime@sha256:current"; got != want {
		t.Fatalf("expected image annotation = %q, want %q", got, want)
	}
	if got, want := session.Metadata[metadataActualImage], "us-docker.pkg.dev/test/agent-runtime@sha256:current"; got != want {
		t.Fatalf("actual image metadata = %q, want %q", got, want)
	}
	if got, want := session.Metadata[metadataImageMatch], "true"; got != want {
		t.Fatalf("image match metadata = %q, want %q", got, want)
	}
	if session.DrainAt == nil || session.ExpiresAt == nil {
		t.Fatalf("session lifecycle = (%v, %v, %v), want drain/expires", session.StartedAt, session.DrainAt, session.ExpiresAt)
	}
}

func TestRuntimeContractRejectsStaleSandboxPodImage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	runtime := &kubernetesSandboxRuntime{
		cfg: Config{
			Namespace:           "runtime-system",
			Container:           "runtime",
			SandboxReadyTimeout: 2 * time.Second,
			CleanupTimeout:      2 * time.Second,
			SessionTTL:          2 * time.Hour,
			SessionDrainBefore:  5 * time.Minute,
			WarmPool:            "none",
		},
		core:   k8sfake.NewSimpleClientset(),
		agents: agentfake.NewSimpleClientset(),
		extensions: extfake.NewSimpleClientset(&extv1alpha1.SandboxTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "agent-runtime",
				Namespace: "runtime-system",
			},
			Spec: extv1alpha1.SandboxTemplateSpec{
				PodTemplate: sandboxv1alpha1.PodTemplate{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:  "runtime",
						Image: "us-docker.pkg.dev/test/agent-runtime@sha256:current",
					}}},
				},
			},
		}),
	}
	readyErr := make(chan error, 1)
	go func() {
		readyErr <- seedReadyClaimSandbox(ctx, runtime, "runtime-system", "session-2", "session-2-sandbox", "session-2-pod", "us-docker.pkg.dev/test/agent-runtime@sha256:old")
	}()

	_, err := runtime.Start(ctx, startSandboxRequest{
		Name:       "session-2",
		PluginName: "agent-provider",
		Namespace:  "runtime-system",
		Template:   "agent-runtime",
	})
	if err == nil || !strings.Contains(err.Error(), "stale gke agent sandbox runtime session") {
		t.Fatalf("Start error = %v, want stale runtime session", err)
	}
	if err := <-readyErr; err != nil {
		t.Fatalf("seed ready claim: %v", err)
	}
	if _, err := runtime.extensions.ExtensionsV1alpha1().SandboxClaims("runtime-system").Get(ctx, "session-2", metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("stale SandboxClaim still exists err = %v, want not found", err)
	}
}

func TestRuntimeContractResolvesClaimSandboxByClaimNameFallback(t *testing.T) {
	t.Parallel()

	runtime := &kubernetesSandboxRuntime{
		cfg:    Config{SandboxReadyTimeout: time.Second},
		core:   k8sfake.NewSimpleClientset(),
		agents: agentfake.NewSimpleClientset(),
		extensions: extfake.NewSimpleClientset(
			&extv1alpha1.SandboxClaim{
				TypeMeta: metav1.TypeMeta{
					APIVersion: extv1alpha1.SchemeGroupVersion.String(),
					Kind:       "SandboxClaim",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claim-1",
					Namespace: "runtime-system",
					UID:       "claim-uid-1",
				},
				Status: extv1alpha1.SandboxClaimStatus{
					Conditions: []metav1.Condition{{
						Type:   "Ready",
						Status: metav1.ConditionTrue,
					}},
					SandboxStatus: extv1alpha1.SandboxStatus{},
				},
			},
		),
	}

	_, err := runtime.agents.AgentsV1alpha1().Sandboxes("runtime-system").Create(context.Background(), &sandboxv1alpha1.Sandbox{
		TypeMeta: metav1.TypeMeta{
			APIVersion: sandboxv1alpha1.SchemeGroupVersion.String(),
			Kind:       "Sandbox",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim-1",
			Namespace: "runtime-system",
		},
		Status: sandboxv1alpha1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1alpha1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("seed Sandbox Create: %v", err)
	}
	if _, err := runtime.agents.AgentsV1alpha1().Sandboxes("runtime-system").Get(context.Background(), "claim-1", metav1.GetOptions{}); err != nil {
		t.Fatalf("seed Sandbox Get: %v", err)
	}

	ready, err := runtime.waitForClaimReady(context.Background(), sandboxHandle{
		Name:      "session-1",
		Namespace: "runtime-system",
		Mode:      "claim",
		ClaimName: "claim-1",
	})
	if err != nil {
		t.Fatalf("waitForClaimReady: %v", err)
	}
	if got, want := ready.SandboxName, "claim-1"; got != want {
		t.Fatalf("SandboxName = %q, want %q", got, want)
	}
	if got, want := ready.PodName, "claim-1"; got != want {
		t.Fatalf("PodName = %q, want %q", got, want)
	}
	if !ready.Ready {
		t.Fatalf("Ready = false, want true")
	}
}

func TestRuntimeContractPluginStartLeaseIsExclusiveAndExpires(t *testing.T) {
	t.Parallel()

	runtime := &kubernetesSandboxRuntime{
		core: k8sfake.NewSimpleClientset(),
	}
	handle := sandboxHandle{
		Name:      "session-1",
		Namespace: "runtime-system",
	}
	ctx := context.Background()

	if err := runtime.AcquirePluginStartLease(ctx, handle, "holder-a", time.Minute); err != nil {
		t.Fatalf("AcquirePluginStartLease holder-a: %v", err)
	}
	if err := runtime.AcquirePluginStartLease(ctx, handle, "holder-b", time.Minute); !errors.Is(err, errPluginAlreadyStarted) {
		t.Fatalf("AcquirePluginStartLease holder-b error = %v, want errPluginAlreadyStarted", err)
	}

	leases := runtime.core.CoordinationV1().Leases("runtime-system")
	lease, err := leases.Get(ctx, pluginStartLeaseName(handle), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get Lease: %v", err)
	}
	expiredAt := metav1.MicroTime{Time: time.Now().Add(-2 * time.Minute)}
	durationSeconds := int32(1)
	lease.Spec.LeaseDurationSeconds = &durationSeconds
	lease.Spec.AcquireTime = &expiredAt
	lease.Spec.RenewTime = &expiredAt
	if _, err := leases.Update(ctx, lease, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("expire Lease: %v", err)
	}

	if err := runtime.AcquirePluginStartLease(ctx, handle, "holder-b", time.Second); err != nil {
		t.Fatalf("AcquirePluginStartLease holder-b after expiry: %v", err)
	}
	if err := runtime.ReleasePluginStartLease(ctx, handle, "holder-a"); err != nil {
		t.Fatalf("ReleasePluginStartLease stale holder: %v", err)
	}
	lease, err = leases.Get(ctx, pluginStartLeaseName(handle), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get Lease after stale release: %v", err)
	}
	if got, want := derefString(lease.Spec.HolderIdentity), "holder-b"; got != want {
		t.Fatalf("Lease holder after stale release = %q, want %q", got, want)
	}

	if err := runtime.ReleasePluginStartLease(ctx, handle, "holder-b"); err != nil {
		t.Fatalf("ReleasePluginStartLease holder-b: %v", err)
	}
	if _, err := leases.Get(ctx, pluginStartLeaseName(handle), metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("Get Lease after release error = %v, want NotFound", err)
	}
}

func TestRuntimeContractListsKubernetesBackedSessions(t *testing.T) {
	t.Parallel()

	managedLabels := func(sessionID, pluginName string) map[string]string {
		return map[string]string{
			"app.kubernetes.io/managed-by": "gestalt",
			"gestalt.dev/runtime":          "gke-agent-sandbox",
			"gestalt.dev/plugin":           pluginName,
			runtimeSessionLabel:            sessionID,
		}
	}
	runtime := &kubernetesSandboxRuntime{
		core: k8sfake.NewSimpleClientset(
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "claim-sandbox", Namespace: "runtime-system"},
				Status:     corev1.PodStatus{PodIP: "10.20.0.11"},
			},
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "direct-session", Namespace: "runtime-system"},
				Status:     corev1.PodStatus{PodIP: "10.20.0.12"},
			},
		),
		agents:     agentfake.NewSimpleClientset(),
		extensions: extfake.NewSimpleClientset(),
	}
	ctx := context.Background()
	if _, err := runtime.extensions.ExtensionsV1alpha1().SandboxClaims("runtime-system").Create(ctx, &extv1alpha1.SandboxClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: extv1alpha1.SchemeGroupVersion.String(),
			Kind:       "SandboxClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim-session",
			Namespace: "runtime-system",
			UID:       "claim-uid",
			Labels:    managedLabels("claim-session", "linear"),
			Annotations: map[string]string{
				sessionPluginAnnotation:   "linear",
				sessionTemplateAnnotation: "agent-runtime",
				sessionMetadataAnnotation: `{"tenant":"claim"}`,
			},
		},
		Status: extv1alpha1.SandboxClaimStatus{
			SandboxStatus: extv1alpha1.SandboxStatus{Name: "claim-sandbox"},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create SandboxClaim: %v", err)
	}
	if _, err := runtime.agents.AgentsV1alpha1().Sandboxes("runtime-system").Create(ctx, &sandboxv1alpha1.Sandbox{
		TypeMeta: metav1.TypeMeta{
			APIVersion: sandboxv1alpha1.SchemeGroupVersion.String(),
			Kind:       "Sandbox",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim-sandbox",
			Namespace: "runtime-system",
			Labels:    managedLabels("claim-session", "linear"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: extv1alpha1.SchemeGroupVersion.String(),
				Kind:       "SandboxClaim",
				Name:       "claim-session",
				UID:        "claim-uid",
			}},
		},
		Status: sandboxv1alpha1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1alpha1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			}},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create claim backing Sandbox: %v", err)
	}
	if _, err := runtime.agents.AgentsV1alpha1().Sandboxes("runtime-system").Create(ctx, &sandboxv1alpha1.Sandbox{
		TypeMeta: metav1.TypeMeta{
			APIVersion: sandboxv1alpha1.SchemeGroupVersion.String(),
			Kind:       "Sandbox",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "direct-session",
			Namespace: "runtime-system",
			Labels:    managedLabels("direct-session", "github"),
			Annotations: map[string]string{
				sessionPluginAnnotation:   "github",
				sessionMetadataAnnotation: `{"tenant":"direct"}`,
			},
		},
		Status: sandboxv1alpha1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1alpha1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			}},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create direct Sandbox: %v", err)
	}
	if err := runtime.AcquirePluginStartLease(ctx, sandboxHandle{Name: "direct-session", Namespace: "runtime-system"}, "holder", time.Minute); err != nil {
		t.Fatalf("AcquirePluginStartLease direct session: %v", err)
	}

	sessions, err := runtime.ListSessions(ctx, "runtime-system")
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	byID := map[string]sandboxSession{}
	for _, session := range sessions {
		byID[session.ID] = session
	}
	if got, want := len(byID), 2; got != want {
		t.Fatalf("ListSessions returned %d sessions, want %d: %#v", got, want, sessions)
	}

	claimSession := byID["claim-session"]
	if claimSession.ID == "" {
		t.Fatalf("ListSessions missing claim-backed session")
	}
	if got, want := claimSession.Handle.Mode, "claim"; got != want {
		t.Fatalf("claim session mode = %q, want %q", got, want)
	}
	if got, want := claimSession.Handle.SandboxName, "claim-sandbox"; got != want {
		t.Fatalf("claim session sandbox = %q, want %q", got, want)
	}
	if got, want := claimSession.Metadata["tenant"], "claim"; got != want {
		t.Fatalf("claim session tenant = %q, want %q", got, want)
	}
	if got, want := claimSession.PluginName, "linear"; got != want {
		t.Fatalf("claim session plugin = %q, want %q", got, want)
	}

	directSession := byID["direct-session"]
	if directSession.ID == "" {
		t.Fatalf("ListSessions missing direct session")
	}
	if got, want := directSession.Handle.Mode, "sandbox"; got != want {
		t.Fatalf("direct session mode = %q, want %q", got, want)
	}
	if got, want := directSession.Metadata["tenant"], "direct"; got != want {
		t.Fatalf("direct session tenant = %q, want %q", got, want)
	}
	if !directSession.PluginStarting {
		t.Fatalf("direct session PluginStarting = false, want true while Lease is active")
	}
}

func TestRuntimeContractResolveSessionRejectsAmbiguousLabelMatches(t *testing.T) {
	t.Parallel()

	runtime := &kubernetesSandboxRuntime{
		core:       k8sfake.NewSimpleClientset(),
		agents:     agentfake.NewSimpleClientset(),
		extensions: extfake.NewSimpleClientset(),
	}
	ctx := context.Background()
	for _, name := range []string{"direct-a", "direct-b"} {
		if _, err := runtime.agents.AgentsV1alpha1().Sandboxes("runtime-system").Create(ctx, &sandboxv1alpha1.Sandbox{
			TypeMeta: metav1.TypeMeta{
				APIVersion: sandboxv1alpha1.SchemeGroupVersion.String(),
				Kind:       "Sandbox",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "runtime-system",
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "gestalt",
					"gestalt.dev/runtime":          "gke-agent-sandbox",
					runtimeSessionLabel:            "shared-session",
				},
			},
			Status: sandboxv1alpha1.SandboxStatus{
				Conditions: []metav1.Condition{{
					Type:   string(sandboxv1alpha1.SandboxConditionReady),
					Status: metav1.ConditionTrue,
				}},
			},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Create Sandbox %s: %v", name, err)
		}
	}

	_, err := runtime.ResolveSession(ctx, "runtime-system", "shared-session")
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ResolveSession error = %v, want ambiguous session error", err)
	}
}

func TestRuntimeContractPodIPDialTargetUsesSandboxPodIP(t *testing.T) {
	t.Parallel()

	runtime := &kubernetesSandboxRuntime{
		core: k8sfake.NewSimpleClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sandbox-pod",
				Namespace: "runtime-system",
			},
			Status: corev1.PodStatus{
				PodIP: "10.20.0.44",
			},
		}),
	}

	tunnel, err := runtime.PodIPDialTarget(context.Background(), sandboxHandle{
		Namespace: "runtime-system",
		PodName:   "sandbox-pod",
	}, 50051)
	if err != nil {
		t.Fatalf("PodIPDialTarget: %v", err)
	}
	if got, want := tunnel.DialTarget(), "tcp://10.20.0.44:50051"; got != want {
		t.Fatalf("DialTarget = %q, want %q", got, want)
	}
	if err := tunnel.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRuntimeContractServiceDNSDialTargetUsesSandboxService(t *testing.T) {
	t.Parallel()

	runtime := &kubernetesSandboxRuntime{}

	tunnel, err := runtime.ServiceDNSDialTarget(context.Background(), sandboxHandle{
		Namespace:   "runtime-system",
		SandboxName: "sandbox-service",
	}, 50051)
	if err != nil {
		t.Fatalf("ServiceDNSDialTarget: %v", err)
	}
	if got, want := tunnel.DialTarget(), "tcp://sandbox-service.runtime-system.svc.cluster.local:50051"; got != want {
		t.Fatalf("DialTarget = %q, want %q", got, want)
	}
	if err := tunnel.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRuntimeContractListSessionsBulkPathIsBounded(t *testing.T) {
	t.Parallel()

	const namespace = "runtime-system"
	const sessionCount = 50

	managedLabels := func(sessionID, pluginName string) map[string]string {
		return map[string]string{
			"app.kubernetes.io/managed-by": "gestalt",
			"gestalt.dev/runtime":          "gke-agent-sandbox",
			"gestalt.dev/plugin":           pluginName,
			runtimeSessionLabel:            sessionID,
		}
	}

	core := k8sfake.NewSimpleClientset()
	agents := agentfake.NewSimpleClientset()
	extensions := extfake.NewSimpleClientset()
	runtime := &kubernetesSandboxRuntime{
		core:       core,
		agents:     agents,
		extensions: extensions,
	}
	ctx := context.Background()

	deletionMarkerID := "session-7"
	now := metav1.Now()

	for i := 0; i < sessionCount; i++ {
		sessionID := "session-" + strconv.Itoa(i)
		sandboxName := sessionID + "-sandbox"
		podName := sessionID + "-pod"
		claimUID := types.UID("claim-uid-" + strconv.Itoa(i))

		if _, err := extensions.ExtensionsV1alpha1().SandboxClaims(namespace).Create(ctx, &extv1alpha1.SandboxClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sessionID,
				Namespace: namespace,
				UID:       claimUID,
				Labels:    managedLabels(sessionID, "linear"),
				Annotations: map[string]string{
					sessionPluginAnnotation: "linear",
				},
			},
			Status: extv1alpha1.SandboxClaimStatus{
				SandboxStatus: extv1alpha1.SandboxStatus{Name: sandboxName},
			},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Create SandboxClaim %s: %v", sessionID, err)
		}
		if _, err := agents.AgentsV1alpha1().Sandboxes(namespace).Create(ctx, &sandboxv1alpha1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sandboxName,
				Namespace: namespace,
				Labels:    managedLabels(sessionID, "linear"),
				Annotations: map[string]string{
					sandboxv1alpha1.SandboxPodNameAnnotation: podName,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: extv1alpha1.SchemeGroupVersion.String(),
					Kind:       "SandboxClaim",
					Name:       sessionID,
					UID:        claimUID,
				}},
			},
			Status: sandboxv1alpha1.SandboxStatus{
				Conditions: []metav1.Condition{{
					Type:   string(sandboxv1alpha1.SandboxConditionReady),
					Status: metav1.ConditionTrue,
				}},
			},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Create Sandbox %s: %v", sandboxName, err)
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
			Status:     corev1.PodStatus{PodIP: "10.30.0." + strconv.Itoa(i+1)},
		}
		if sessionID == deletionMarkerID {
			pod.ObjectMeta.DeletionTimestamp = &now
			pod.ObjectMeta.Finalizers = []string{"gestalt.dev/test-finalizer"}
		}
		if _, err := core.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Create Pod %s: %v", podName, err)
		}
	}

	core.ClearActions()
	agents.ClearActions()
	extensions.ClearActions()

	sessions, err := runtime.ListSessions(ctx, namespace)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if got, want := len(sessions), sessionCount; got != want {
		t.Fatalf("ListSessions returned %d sessions, want %d", got, want)
	}

	totalActions := len(core.Actions()) + len(agents.Actions()) + len(extensions.Actions())
	if totalActions > 5 {
		t.Fatalf("ListSessions issued %d kube actions for %d sessions, want <= 5 (claims+sandboxes+pods+leases+templates)", totalActions, sessionCount)
	}

	byID := map[string]sandboxSession{}
	for _, session := range sessions {
		byID[session.ID] = session
	}
	deleted, ok := byID[deletionMarkerID]
	if !ok {
		t.Fatalf("ListSessions missing session %q", deletionMarkerID)
	}
	if deleted.Handle.PodIP != "" {
		t.Fatalf("session %q PodIP = %q, want empty when pod has DeletionTimestamp", deletionMarkerID, deleted.Handle.PodIP)
	}
	live, ok := byID["session-0"]
	if !ok {
		t.Fatalf("ListSessions missing session-0")
	}
	if live.Handle.PodIP == "" {
		t.Fatalf("session-0 PodIP = empty, want a pod IP from cache")
	}
}

func assertPolicyRule(t *testing.T, rule networkingv1.NetworkPolicyEgressRule, wantCIDRs []string, wantPort int32, wantProtocol corev1.Protocol) {
	t.Helper()

	if len(rule.Ports) != 1 {
		t.Fatalf("egress rule ports = %d, want 1", len(rule.Ports))
	}
	port := rule.Ports[0]
	if port.Port == nil || port.Port.IntVal != wantPort {
		t.Fatalf("egress rule port = %#v, want %d", port.Port, wantPort)
	}
	if port.Protocol == nil || *port.Protocol != wantProtocol {
		t.Fatalf("egress rule protocol = %v, want %v", port.Protocol, wantProtocol)
	}
	if len(rule.To) != len(wantCIDRs) {
		t.Fatalf("egress rule peers = %d, want %d", len(rule.To), len(wantCIDRs))
	}
	for i, want := range wantCIDRs {
		if rule.To[i].IPBlock == nil || rule.To[i].IPBlock.CIDR != want {
			t.Fatalf("egress rule peer %d = %#v, want CIDR %q", i, rule.To[i].IPBlock, want)
		}
	}
}
