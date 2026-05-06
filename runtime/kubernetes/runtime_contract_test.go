package kubernetes

import (
	"context"
	"errors"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestRuntimeContractStartsSessionFromPodTemplate(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-runtime", Namespace: "runtime-system"},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{"template-label": "kept"},
				Annotations: map[string]string{"template-annotation": "kept"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "runtime",
					Image: "registry.example/base:old",
					Ports: []corev1.ContainerPort{{Name: "plugin-grpc", ContainerPort: 50051}},
				}},
			},
		},
	})
	markCreatedPodsReady(client)
	runtime := &kubernetesRuntime{
		cfg:  testConfig(),
		core: client,
	}

	session, err := runtime.Start(ctx, startRuntimeSessionRequest{
		Name:       "session-1",
		PluginName: "claude",
		Namespace:  "runtime-system",
		Template:   "agent-runtime",
		Image:      "registry.example/runtime@sha256:abc",
		Metadata:   map[string]string{"tenant": "prod"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got, want := session.ID, "session-1"; got != want {
		t.Fatalf("session ID = %q, want %q", got, want)
	}
	if !session.Handle.Ready {
		t.Fatalf("session ready = false, want true")
	}

	pod, err := client.CoreV1().Pods("runtime-system").Get(ctx, "session-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get Pod: %v", err)
	}
	if got, want := pod.Labels["template-label"], "kept"; got != want {
		t.Fatalf("template label = %q, want %q", got, want)
	}
	if got, want := pod.Labels["gestalt.dev/runtime"], "kubernetes"; got != want {
		t.Fatalf("runtime label = %q, want %q", got, want)
	}
	if got, want := pod.Labels[runtimeSessionLabel], "session-1"; got != want {
		t.Fatalf("session label = %q, want %q", got, want)
	}
	if _, ok := pod.Labels["tenant"]; ok {
		t.Fatalf("metadata leaked into labels: %#v", pod.Labels)
	}
	if got, want := pod.Annotations[sessionMetadataAnnotation], `{"tenant":"prod"}`; got != want {
		t.Fatalf("metadata annotation = %q, want %q", got, want)
	}
	if got, want := pod.Spec.Containers[0].Image, "registry.example/runtime@sha256:abc"; got != want {
		t.Fatalf("container image = %q, want %q", got, want)
	}
}

func TestRuntimeContractDirectSessionCreatesImagePullSecretAndService(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	markCreatedPodsReady(client)
	cfg := testConfig()
	cfg.ConnectionMode = connectionModeServiceDNS
	runtime := &kubernetesRuntime{cfg: cfg, core: client}

	_, err := runtime.Start(ctx, startRuntimeSessionRequest{
		Name:             "session-2",
		PluginName:       "github",
		Namespace:        "runtime-system",
		Image:            "registry.example/runtime:latest",
		DockerConfigJSON: `{"auths":{"registry.example":{"username":"u","password":"p"}}}`,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	pod, err := client.CoreV1().Pods("runtime-system").Get(ctx, "session-2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get Pod: %v", err)
	}
	if len(pod.Spec.ImagePullSecrets) != 1 {
		t.Fatalf("imagePullSecrets = %#v, want one", pod.Spec.ImagePullSecrets)
	}
	secretName := pod.Spec.ImagePullSecrets[0].Name
	secret, err := client.CoreV1().Secrets("runtime-system").Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get Secret: %v", err)
	}
	if got, want := string(secret.Data[corev1.DockerConfigJsonKey]), `{"auths":{"registry.example":{"username":"u","password":"p"}}}`; got != want {
		t.Fatalf("dockerconfigjson = %q, want %q", got, want)
	}
	service, err := client.CoreV1().Services("runtime-system").Get(ctx, "session-2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get Service: %v", err)
	}
	if got, want := service.Spec.Selector[runtimeSessionLabel], "session-2"; got != want {
		t.Fatalf("service selector = %q, want %q", got, want)
	}
	if got, want := service.Spec.Selector["gestalt.dev/runtime"], "kubernetes"; got != want {
		t.Fatalf("service runtime selector = %q, want %q", got, want)
	}
}

func TestRuntimeContractPluginStartLeaseRejectsConcurrentStart(t *testing.T) {
	ctx := context.Background()
	runtime := &kubernetesRuntime{cfg: testConfig(), core: fake.NewSimpleClientset()}
	handle := runtimeHandle{Name: "session-3", Namespace: "runtime-system", PodName: "session-3", Ready: true}

	if err := runtime.AcquirePluginStartLease(ctx, handle, "holder-a", time.Minute); err != nil {
		t.Fatalf("AcquirePluginStartLease holder-a: %v", err)
	}
	err := runtime.AcquirePluginStartLease(ctx, handle, "holder-b", time.Minute)
	if !errors.Is(err, errPluginAlreadyStarted) {
		t.Fatalf("AcquirePluginStartLease holder-b error = %v, want errPluginAlreadyStarted", err)
	}
	if err := runtime.ReleasePluginStartLease(ctx, handle, "holder-a"); err != nil {
		t.Fatalf("ReleasePluginStartLease: %v", err)
	}
	if err := runtime.AcquirePluginStartLease(ctx, handle, "holder-b", time.Minute); err != nil {
		t.Fatalf("AcquirePluginStartLease holder-b after release: %v", err)
	}
}

func TestRuntimeContractTerminalPodMapsToFailedSession(t *testing.T) {
	ctx := context.Background()
	runtime := &kubernetesRuntime{
		cfg: testConfig(),
		core: fake.NewSimpleClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "session-failed",
				Namespace: "runtime-system",
				Labels:    runtimeLabels("github", "session-failed"),
			},
			Status: corev1.PodStatus{Phase: corev1.PodFailed},
		}),
	}
	session, err := runtime.ResolveSession(ctx, "runtime-system", "session-failed")
	if err != nil {
		t.Fatalf("ResolveSession: %v", err)
	}
	if !session.Failed {
		t.Fatalf("session Failed = false, want true")
	}
	if got, want := sessionStateForRuntime(ctx, runtime, session), sessionStateFailed; got != want {
		t.Fatalf("session state = %q, want %q", got, want)
	}
}

func TestRuntimeContractResolveSessionRejectsUnmanagedPodByName(t *testing.T) {
	ctx := context.Background()
	runtime := &kubernetesRuntime{
		cfg: testConfig(),
		core: fake.NewSimpleClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "session-unmanaged",
				Namespace: "runtime-system",
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}),
	}

	if _, err := runtime.ResolveSession(ctx, "runtime-system", "session-unmanaged"); err == nil {
		t.Fatalf("ResolveSession succeeded for unmanaged Pod, want error")
	}
}

func TestRuntimeContractResolveSessionUsesManagedLabelsForFallbackLookup(t *testing.T) {
	ctx := context.Background()
	runtime := &kubernetesRuntime{
		cfg: testConfig(),
		core: fake.NewSimpleClientset(
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unmanaged-pod",
					Namespace: "runtime-system",
					Labels: map[string]string{
						runtimeSessionLabel: "external-id",
					},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "managed-pod",
					Namespace: "runtime-system",
					Labels:    runtimeLabels("github", "external-id"),
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
		),
	}

	session, err := runtime.ResolveSession(ctx, "runtime-system", "external-id")
	if err != nil {
		t.Fatalf("ResolveSession: %v", err)
	}
	if got, want := session.ID, "managed-pod"; got != want {
		t.Fatalf("session ID = %q, want %q", got, want)
	}
}

func TestRuntimeContractStopSkipsUnmanagedNamedObjects(t *testing.T) {
	ctx := context.Background()
	handle := runtimeHandle{Name: "session-owned", Namespace: "runtime-system", PodName: "session-owned"}
	holder := "external"
	leaseDuration := int32(300)
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: handle.PodName, Namespace: handle.Namespace},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: handle.Name, Namespace: handle.Namespace},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: imagePullSecretName(handle), Namespace: handle.Namespace},
		},
		&networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: hostnameEgressPolicyName(handle), Namespace: handle.Namespace},
		},
		&coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: pluginStartLeaseName(handle), Namespace: handle.Namespace},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &holder,
				LeaseDurationSeconds: &leaseDuration,
			},
		},
	)
	runtime := &kubernetesRuntime{cfg: testConfig(), core: client}

	if err := runtime.Stop(ctx, handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := client.CoreV1().Pods(handle.Namespace).Get(ctx, handle.PodName, metav1.GetOptions{}); err != nil {
		t.Fatalf("unmanaged Pod was deleted: %v", err)
	}
	if _, err := client.CoreV1().Services(handle.Namespace).Get(ctx, handle.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("unmanaged Service was deleted: %v", err)
	}
	if _, err := client.CoreV1().Secrets(handle.Namespace).Get(ctx, imagePullSecretName(handle), metav1.GetOptions{}); err != nil {
		t.Fatalf("unmanaged Secret was deleted: %v", err)
	}
	if _, err := client.NetworkingV1().NetworkPolicies(handle.Namespace).Get(ctx, hostnameEgressPolicyName(handle), metav1.GetOptions{}); err != nil {
		t.Fatalf("unmanaged NetworkPolicy was deleted: %v", err)
	}
	if _, err := client.CoordinationV1().Leases(handle.Namespace).Get(ctx, pluginStartLeaseName(handle), metav1.GetOptions{}); err != nil {
		t.Fatalf("unmanaged Lease was deleted: %v", err)
	}
}

func TestRuntimeContractStopDeletesManagedNamedObjects(t *testing.T) {
	ctx := context.Background()
	handle := runtimeHandle{Name: "session-managed", Namespace: "runtime-system", PodName: "session-managed"}
	holder := "gestalt"
	leaseDuration := int32(300)
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: handle.PodName, Namespace: handle.Namespace, Labels: runtimeLabels("github", handle.Name)},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: handle.Name, Namespace: handle.Namespace, Labels: runtimeLabels("", handle.Name)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: imagePullSecretName(handle), Namespace: handle.Namespace, Labels: runtimeLabels("", handle.Name)},
		},
		&networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: hostnameEgressPolicyName(handle), Namespace: handle.Namespace, Labels: runtimeLabels("", handle.Name)},
		},
		&coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: pluginStartLeaseName(handle), Namespace: handle.Namespace, Labels: runtimeLabels("", handle.Name)},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &holder,
				LeaseDurationSeconds: &leaseDuration,
			},
		},
	)
	runtime := &kubernetesRuntime{cfg: testConfig(), core: client}

	if err := runtime.Stop(ctx, handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := client.CoreV1().Pods(handle.Namespace).Get(ctx, handle.PodName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("managed Pod still exists or get failed with wrong error: %v", err)
	}
	if _, err := client.CoreV1().Services(handle.Namespace).Get(ctx, handle.Name, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("managed Service still exists or get failed with wrong error: %v", err)
	}
	if _, err := client.CoreV1().Secrets(handle.Namespace).Get(ctx, imagePullSecretName(handle), metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("managed Secret still exists or get failed with wrong error: %v", err)
	}
	if _, err := client.NetworkingV1().NetworkPolicies(handle.Namespace).Get(ctx, hostnameEgressPolicyName(handle), metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("managed NetworkPolicy still exists or get failed with wrong error: %v", err)
	}
	if _, err := client.CoordinationV1().Leases(handle.Namespace).Get(ctx, pluginStartLeaseName(handle), metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("managed Lease still exists or get failed with wrong error: %v", err)
	}
}

func TestRuntimeContractHostnameEgressPolicyAllowsProxyAndDNS(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.HostnameEgress.Mode = hostnameEgressModePublicProxy
	runtime := &kubernetesRuntime{
		cfg:  cfg,
		core: fake.NewSimpleClientset(),
		readFile: func(context.Context, runtimeHandle, string) (string, error) {
			return "nameserver 10.0.0.10\n", nil
		},
		lookupIP: func(_ context.Context, host string) ([]net.IPAddr, error) {
			switch host {
			case "proxy.gestalt.example":
				return []net.IPAddr{{IP: net.ParseIP("203.0.113.10")}}, nil
			case "relay.gestalt.example":
				return []net.IPAddr{{IP: net.ParseIP("203.0.113.20")}}, nil
			default:
				t.Fatalf("unexpected hostname lookup %q", host)
				return nil, nil
			}
		},
	}
	handle := runtimeHandle{Name: "session-4", Namespace: "runtime-system", PodName: "session-4"}

	policyName, err := runtime.EnsureHostnameEgressPolicy(ctx, handle, hostnameEgressConfig{
		Endpoints: []hostnameEgressEndpoint{
			{Host: "proxy.gestalt.example", Port: 443},
			{Host: "relay.gestalt.example", Port: 9443},
		},
	})
	if err != nil {
		t.Fatalf("EnsureHostnameEgressPolicy: %v", err)
	}
	policy, err := runtime.core.NetworkingV1().NetworkPolicies("runtime-system").Get(ctx, policyName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get NetworkPolicy: %v", err)
	}
	if got, want := policy.Spec.PodSelector.MatchLabels[runtimeSessionLabel], "session-4"; got != want {
		t.Fatalf("policy selector = %q, want %q", got, want)
	}
	if got, want := policy.Spec.PodSelector.MatchLabels["gestalt.dev/runtime"], "kubernetes"; got != want {
		t.Fatalf("policy runtime selector = %q, want %q", got, want)
	}
	if len(policy.Spec.Egress) != 4 {
		t.Fatalf("egress rules = %d, want DNS TCP/UDP plus two endpoints", len(policy.Spec.Egress))
	}
	assertPolicyRule(t, policy.Spec.Egress[0], []string{"10.0.0.10/32"}, 53, corev1.ProtocolUDP)
	assertPolicyRule(t, policy.Spec.Egress[1], []string{"10.0.0.10/32"}, 53, corev1.ProtocolTCP)
	assertPolicyRule(t, policy.Spec.Egress[2], []string{"203.0.113.10/32"}, 443, corev1.ProtocolTCP)
	assertPolicyRule(t, policy.Spec.Egress[3], []string{"203.0.113.20/32"}, 9443, corev1.ProtocolTCP)
}

func TestRuntimeContractHostnameEgressPolicyRejectsUnmanagedCollision(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.HostnameEgress.Mode = hostnameEgressModePublicProxy
	handle := runtimeHandle{Name: "session-5", Namespace: "runtime-system", PodName: "session-5"}
	runtime := &kubernetesRuntime{
		cfg: cfg,
		core: fake.NewSimpleClientset(&networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: hostnameEgressPolicyName(handle), Namespace: handle.Namespace},
		}),
		readFile: func(context.Context, runtimeHandle, string) (string, error) {
			return "nameserver 10.0.0.10\n", nil
		},
		lookupIP: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("203.0.113.10")}}, nil
		},
	}

	_, err := runtime.EnsureHostnameEgressPolicy(ctx, handle, hostnameEgressConfig{
		Endpoints: []hostnameEgressEndpoint{{Host: "proxy.gestalt.example", Port: 443}},
	})
	if err == nil {
		t.Fatalf("EnsureHostnameEgressPolicy succeeded for unmanaged NetworkPolicy collision, want error")
	}
}

func TestRuntimeContractAcquireLeaseRejectsUnmanagedCollision(t *testing.T) {
	ctx := context.Background()
	handle := runtimeHandle{Name: "session-lease", Namespace: "runtime-system", PodName: "session-lease"}
	holder := "external"
	client := fake.NewSimpleClientset(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: pluginStartLeaseName(handle), Namespace: handle.Namespace},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
	})
	runtime := &kubernetesRuntime{cfg: testConfig(), core: client}

	err := runtime.AcquirePluginStartLease(ctx, handle, "gestalt", time.Minute)
	if err == nil {
		t.Fatalf("AcquirePluginStartLease succeeded for unmanaged Lease collision, want error")
	}
	lease, getErr := client.CoordinationV1().Leases(handle.Namespace).Get(ctx, pluginStartLeaseName(handle), metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("Get Lease: %v", getErr)
	}
	if got, want := derefString(lease.Spec.HolderIdentity), holder; got != want {
		t.Fatalf("Lease holder = %q, want %q", got, want)
	}
}

func TestRuntimeContractMarkPluginStartedRejectsUnmanagedPod(t *testing.T) {
	ctx := context.Background()
	handle := runtimeHandle{Name: "session-marker", Namespace: "runtime-system", PodName: "session-marker"}
	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: handle.PodName, Namespace: handle.Namespace},
	})
	runtime := &kubernetesRuntime{cfg: testConfig(), core: client}

	err := runtime.MarkPluginStarted(ctx, handle, "holder", "github")
	if err == nil {
		t.Fatalf("MarkPluginStarted succeeded for unmanaged Pod, want error")
	}
	pod, getErr := client.CoreV1().Pods(handle.Namespace).Get(ctx, handle.PodName, metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("Get Pod: %v", getErr)
	}
	if got := pod.Annotations[pluginStartedAnnotation]; got != "" {
		t.Fatalf("plugin started annotation = %q, want empty", got)
	}
}

func TestRuntimeContractChildResourceNamesRemainUniqueForLongSessionNames(t *testing.T) {
	sessionA := "gestalt-" + strings.Repeat("very-long-plugin-", 5) + "aaaaaaaa-session-000001"
	sessionB := "gestalt-" + strings.Repeat("very-long-plugin-", 5) + "bbbbbbbb-session-000002"
	handleA := runtimeHandle{Name: sessionA}
	handleB := runtimeHandle{Name: sessionB}

	assertDistinctDNSLabel(t, pluginStartLeaseName(handleA), pluginStartLeaseName(handleB))
	assertDistinctDNSLabel(t, hostnameEgressPolicyName(handleA), hostnameEgressPolicyName(handleB))
	assertDistinctDNSLabel(t, imagePullSecretName(handleA), imagePullSecretName(handleB))
}

func markCreatedPodsReady(client *fake.Clientset) {
	client.Fake.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		create := action.(k8stesting.CreateAction)
		pod := create.GetObject().(*corev1.Pod)
		pod.Status.Phase = corev1.PodRunning
		pod.Status.PodIP = "10.20.30.40"
		pod.Status.Conditions = []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		}}
		return false, nil, nil
	})
}

func assertDistinctDNSLabel(t *testing.T, left, right string) {
	t.Helper()
	if left == right {
		t.Fatalf("resource names collided: %q", left)
	}
	for _, value := range []string{left, right} {
		if len(value) > 63 {
			t.Fatalf("resource name %q length = %d, want <= 63", value, len(value))
		}
		if strings.Trim(value, "-") != value {
			t.Fatalf("resource name %q has leading or trailing hyphen", value)
		}
	}
}

func assertPolicyRule(t *testing.T, rule networkingv1.NetworkPolicyEgressRule, wantCIDRs []string, wantPort int32, wantProtocol corev1.Protocol) {
	t.Helper()
	var gotCIDRs []string
	for _, peer := range rule.To {
		if peer.IPBlock != nil {
			gotCIDRs = append(gotCIDRs, peer.IPBlock.CIDR)
		}
	}
	slices.Sort(gotCIDRs)
	slices.Sort(wantCIDRs)
	if !slices.Equal(gotCIDRs, wantCIDRs) {
		t.Fatalf("policy CIDRs = %#v, want %#v", gotCIDRs, wantCIDRs)
	}
	if len(rule.Ports) != 1 {
		t.Fatalf("policy ports = %#v, want one", rule.Ports)
	}
	if rule.Ports[0].Port == nil || rule.Ports[0].Port.IntVal != wantPort {
		t.Fatalf("policy port = %#v, want %d", rule.Ports[0].Port, wantPort)
	}
	if rule.Ports[0].Protocol == nil || *rule.Ports[0].Protocol != wantProtocol {
		t.Fatalf("policy protocol = %#v, want %s", rule.Ports[0].Protocol, wantProtocol)
	}
}

func testConfig() Config {
	cfg := Config{
		Namespace:           "runtime-system",
		Container:           "runtime",
		PluginPort:          50051,
		ConnectionMode:      connectionModePodIP,
		SessionReadyTimeout: time.Second,
		PluginReadyTimeout:  time.Second,
		ExecTimeout:         time.Second,
		CleanupTimeout:      time.Second,
		PodDefaults: PodDefaultsConfig{
			RuntimeClassName: "gvisor",
			ImagePullPolicy:  imagePullPolicyIfNotPresent,
			CPURequest:       "250m",
			MemoryRequest:    "512Mi",
			CPULimit:         "1",
			MemoryLimit:      "1Gi",
		},
	}
	cfg.Normalize()
	return cfg
}
