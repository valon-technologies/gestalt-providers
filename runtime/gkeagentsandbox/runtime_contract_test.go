package gkeagentsandbox

import (
	"context"
	"errors"
	"net"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	agentfake "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
	extfake "sigs.k8s.io/agent-sandbox/clients/k8s/extensions/clientset/versioned/fake"
	extv1alpha1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"
)

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
