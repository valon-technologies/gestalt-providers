package gkeagentsandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/client-go/util/retry"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	agentclientset "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned"
	extclientset "sigs.k8s.io/agent-sandbox/clients/k8s/extensions/clientset/versioned"
	extv1alpha1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"
)

type sandboxRuntime interface {
	HealthCheck(context.Context) error
	Start(context.Context, startSandboxRequest) (sandboxSession, error)
	ResolveSession(context.Context, string, string) (sandboxSession, error)
	ListSessions(context.Context, string) ([]sandboxSession, error)
	Stop(context.Context, sandboxHandle) error
	Exec(context.Context, sandboxHandle, []string, io.Reader) error
	ForwardPort(context.Context, sandboxHandle, int) (tunnel, error)
	PodIPDialTarget(context.Context, sandboxHandle, int) (tunnel, error)
	ServiceDNSDialTarget(context.Context, sandboxHandle, int) (tunnel, error)
	EnsureHostnameEgressPolicy(context.Context, sandboxHandle, hostnameEgressConfig) (string, error)
	DeleteHostnameEgressPolicy(context.Context, sandboxHandle, string) error
	AcquirePluginStartLease(context.Context, sandboxHandle, string, time.Duration) error
	ReleasePluginStartLease(context.Context, sandboxHandle, string) error
	MarkPluginStarted(context.Context, sandboxHandle, string, string) error
	VerifySessionCompatible(context.Context, sandboxSession) error
	Close() error
}

type tunnel interface {
	DialTarget() string
	Close() error
}

var errStaleRuntimeSession = errors.New("stale gke agent sandbox runtime session")

type startSandboxRequest struct {
	Name       string
	AppName string
	Namespace  string
	Template   string
	Image      string
	Metadata   map[string]string
}

type sandboxSession struct {
	ID             string
	AppName     string
	Template       string
	Metadata       map[string]string
	Handle         sandboxHandle
	PluginStarting bool
	PluginStarted  bool
	StartedAt      *time.Time
	DrainAt        *time.Time
	ExpiresAt      *time.Time
}

type sandboxHandle struct {
	Name        string
	Namespace   string
	Mode        string
	ClaimName   string
	SandboxName string
	PodName     string
	Ready       bool
}

type kubernetesSandboxRuntime struct {
	cfg        Config
	restConfig *rest.Config
	core       kubernetes.Interface
	agents     agentclientset.Interface
	extensions extclientset.Interface
	lookupIP   func(context.Context, string) ([]net.IPAddr, error)
	readFile   func(context.Context, sandboxHandle, string) (string, error)
}

// kubernetesClientQPS and kubernetesClientBurst raise the per-process kube
// client rate limits from the client-go defaults of 5 QPS / 10 burst. The
// shared kube client is hit by ListSessions LISTs, hosted-runtime-pool
// reconcile GETs, and per-session GetSession calls; under the defaults a
// modestly active deployment saturated the limiter and ListSessions queued
// behind unrelated GETs long enough to miss its 15s deadline.
const (
	kubernetesClientQPS   = 50
	kubernetesClientBurst = 100
)

func newKubernetesSandboxRuntime(cfg Config) (sandboxRuntime, error) {
	restConfig, err := kubernetesRESTConfig(cfg)
	if err != nil {
		return nil, err
	}
	if restConfig.QPS == 0 {
		restConfig.QPS = kubernetesClientQPS
	}
	if restConfig.Burst == 0 {
		restConfig.Burst = kubernetesClientBurst
	}
	core, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("gke agent sandbox runtime: create kubernetes client: %w", err)
	}
	agents, err := agentclientset.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("gke agent sandbox runtime: create agent sandbox client: %w", err)
	}
	extensions, err := extclientset.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("gke agent sandbox runtime: create agent sandbox extensions client: %w", err)
	}
	return &kubernetesSandboxRuntime{
		cfg:        cfg,
		restConfig: rest.CopyConfig(restConfig),
		core:       core,
		agents:     agents,
		extensions: extensions,
		lookupIP:   net.DefaultResolver.LookupIPAddr,
	}, nil
}

func kubernetesRESTConfig(cfg Config) (*rest.Config, error) {
	overrides := &clientcmd.ConfigOverrides{}
	if cfg.Context != "" {
		overrides.CurrentContext = cfg.Context
	}
	if cfg.Kubeconfig != "" {
		loader := &clientcmd.ClientConfigLoadingRules{ExplicitPath: cfg.Kubeconfig}
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
	}
	if cfg.GKE.IsConfigured() {
		return gkeKubernetesRESTConfig(context.Background(), cfg.GKE)
	}
	if restConfig, err := rest.InClusterConfig(); err == nil {
		return restConfig, nil
	}
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
}

func gkeKubernetesRESTConfig(ctx context.Context, cfg GKEConfig) (*rest.Config, error) {
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	client, err := container.NewClusterManagerClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("gke agent sandbox runtime: create GKE cluster client: %w", err)
	}
	defer client.Close()

	cluster, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: cfg.clusterResourceName()})
	if err != nil {
		return nil, fmt.Errorf("gke agent sandbox runtime: get GKE cluster %s: %w", cfg.clusterResourceName(), err)
	}
	tokenSource, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("gke agent sandbox runtime: load Google application default credentials: %w", err)
	}
	return gkeRESTConfigFromCluster(cfg, cluster, tokenSource)
}

func gkeRESTConfigFromCluster(cfg GKEConfig, cluster *containerpb.Cluster, tokenSource oauth2.TokenSource) (*rest.Config, error) {
	if cluster == nil {
		return nil, fmt.Errorf("gke agent sandbox runtime: GKE cluster response is empty")
	}
	endpoint := strings.TrimSpace(cluster.GetEndpoint())
	if cfg.Endpoint == gkeEndpointPrivate {
		endpoint = strings.TrimSpace(cluster.GetPrivateClusterConfig().GetPrivateEndpoint())
	}
	if endpoint == "" {
		return nil, fmt.Errorf("gke agent sandbox runtime: GKE cluster %s endpoint %q is empty", cfg.clusterResourceName(), cfg.Endpoint)
	}
	if strings.HasPrefix(endpoint, "http://") {
		return nil, fmt.Errorf("gke agent sandbox runtime: GKE cluster %s endpoint must use https", cfg.clusterResourceName())
	}
	if !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	ca := strings.TrimSpace(cluster.GetMasterAuth().GetClusterCaCertificate())
	if ca == "" {
		return nil, fmt.Errorf("gke agent sandbox runtime: GKE cluster %s CA certificate is empty", cfg.clusterResourceName())
	}
	caData, err := base64.StdEncoding.DecodeString(ca)
	if err != nil {
		return nil, fmt.Errorf("gke agent sandbox runtime: decode GKE cluster %s CA certificate: %w", cfg.clusterResourceName(), err)
	}
	if tokenSource == nil {
		return nil, fmt.Errorf("gke agent sandbox runtime: Google token source is required")
	}
	return &rest.Config{
		Host: endpoint,
		TLSClientConfig: rest.TLSClientConfig{
			CAData: caData,
		},
		WrapTransport: func(base http.RoundTripper) http.RoundTripper {
			if base == nil {
				base = http.DefaultTransport
			}
			return &oauth2.Transport{
				Source: tokenSource,
				Base:   base,
			}
		},
	}, nil
}

func (r *kubernetesSandboxRuntime) HealthCheck(ctx context.Context) error {
	_, err := r.core.Discovery().ServerVersion()
	if err != nil {
		return fmt.Errorf("gke agent sandbox runtime: kubernetes discovery failed: %w", err)
	}
	return ctx.Err()
}

func (r *kubernetesSandboxRuntime) Start(ctx context.Context, req startSandboxRequest) (sandboxSession, error) {
	if strings.TrimSpace(req.Template) != "" {
		return r.startClaim(ctx, req)
	}
	return r.startDirectSandbox(ctx, req)
}

func (r *kubernetesSandboxRuntime) startClaim(ctx context.Context, req startSandboxRequest) (sandboxSession, error) {
	templateMeta, err := r.runtimeTemplateMetadata(ctx, req.Namespace, req.Template)
	if err != nil {
		return sandboxSession{}, err
	}
	req.Metadata = mergeRuntimeMetadata(req.Metadata, templateMeta.metadata())
	var lastErr error
	for attempt := 0; attempt <= r.cfg.StaleSessionRetries; attempt++ {
		attemptReq := req
		if attempt > 0 {
			attemptReq.Name = dnsLabelWithSuffix(req.Name, fmt.Sprintf("retry-%d", attempt))
		}
		objectMeta := runtimeObjectMeta(attemptReq)
		claim := &extv1alpha1.SandboxClaim{
			TypeMeta: metav1.TypeMeta{
				APIVersion: extv1alpha1.SchemeGroupVersion.String(),
				Kind:       "SandboxClaim",
			},
			ObjectMeta: objectMeta,
			Spec: extv1alpha1.SandboxClaimSpec{
				TemplateRef: extv1alpha1.SandboxTemplateRef{Name: attemptReq.Template},
			},
		}
		if warmPool := r.sandboxClaimWarmPool(); warmPool != nil {
			claim.Spec.WarmPool = warmPool
		}
		if lifecycle := r.sandboxClaimLifecycle(time.Now()); lifecycle != nil {
			claim.Spec.Lifecycle = lifecycle
		}
		claim.Spec.AdditionalPodMetadata = sandboxv1alpha1.PodMetadata{
			Labels:      cloneStringMap(objectMeta.Labels),
			Annotations: cloneStringMap(objectMeta.Annotations),
		}
		if _, err := r.extensions.ExtensionsV1alpha1().SandboxClaims(attemptReq.Namespace).Create(ctx, claim, metav1.CreateOptions{}); err != nil {
			return sandboxSession{}, fmt.Errorf("create SandboxClaim %s/%s: %w", attemptReq.Namespace, attemptReq.Name, err)
		}
		handle := sandboxHandle{
			Name:      attemptReq.Name,
			Namespace: attemptReq.Namespace,
			Mode:      "claim",
			ClaimName: attemptReq.Name,
		}
		ready, err := r.waitForClaimReady(ctx, handle)
		if err != nil {
			return sandboxSession{}, errors.Join(err, r.cleanupCreatedSandbox(handle))
		}
		session := sandboxSessionFromRuntimeObject(attemptReq.Name, attemptReq.AppName, attemptReq.Template, attemptReq.Metadata, objectMeta.Annotations, ready)
		if err := r.enrichSessionFromClaim(ctx, &session, claim); err != nil {
			return sandboxSession{}, errors.Join(err, r.cleanupCreatedSandbox(ready))
		}
		if err := r.VerifySessionCompatible(ctx, session); err != nil {
			lastErr = err
			_ = r.cleanupCreatedSandbox(ready)
			if attempt < r.cfg.StaleSessionRetries {
				continue
			}
			return sandboxSession{}, err
		}
		return session, nil
	}
	if lastErr != nil {
		return sandboxSession{}, lastErr
	}
	return sandboxSession{}, fmt.Errorf("start SandboxClaim %s/%s: stale session retry loop exhausted", req.Namespace, req.Name)
}

func (r *kubernetesSandboxRuntime) startDirectSandbox(ctx context.Context, req startSandboxRequest) (sandboxSession, error) {
	if strings.TrimSpace(req.Image) == "" {
		return sandboxSession{}, fmt.Errorf("image is required for direct Sandbox sessions")
	}
	replicas := int32(1)
	podSpec, err := r.directPodSpec(req.Image)
	if err != nil {
		return sandboxSession{}, err
	}
	objectMeta := runtimeObjectMeta(req)
	podLabels := runtimeLabels(req.AppName)
	sessionLabel := sanitizeLabelValue(req.Name)
	if sessionLabel != "" {
		podLabels[runtimeSessionLabel] = sessionLabel
	}
	sandbox := &sandboxv1alpha1.Sandbox{
		TypeMeta: metav1.TypeMeta{
			APIVersion: sandboxv1alpha1.SchemeGroupVersion.String(),
			Kind:       "Sandbox",
		},
		ObjectMeta: objectMeta,
		Spec: sandboxv1alpha1.SandboxSpec{
			Replicas: &replicas,
			PodTemplate: sandboxv1alpha1.PodTemplate{
				ObjectMeta: sandboxv1alpha1.PodMetadata{
					Labels: podLabels,
				},
				Spec: podSpec,
			},
		},
	}
	if _, err := r.agents.AgentsV1alpha1().Sandboxes(req.Namespace).Create(ctx, sandbox, metav1.CreateOptions{}); err != nil {
		return sandboxSession{}, fmt.Errorf("create Sandbox %s/%s: %w", req.Namespace, req.Name, err)
	}
	handle := sandboxHandle{
		Name:        req.Name,
		Namespace:   req.Namespace,
		Mode:        "sandbox",
		SandboxName: req.Name,
	}
	ready, err := r.waitForSandboxReady(ctx, handle)
	if err != nil {
		return sandboxSession{}, errors.Join(err, r.cleanupCreatedSandbox(handle))
	}
	return sandboxSessionFromRuntimeObject(req.Name, req.AppName, req.Template, req.Metadata, objectMeta.Annotations, ready), nil
}

func (r *kubernetesSandboxRuntime) directPodSpec(image string) (corev1.PodSpec, error) {
	falseValue := false
	trueValue := true
	runtimeClassName := r.cfg.Direct.RuntimeClassName
	runAsUser := r.cfg.Direct.RunAsUser
	if runAsUser == nil {
		value := defaultRunAsUser
		runAsUser = &value
	}
	command := append([]string(nil), r.cfg.Direct.Command...)
	args := append([]string(nil), r.cfg.Direct.Args...)
	if len(command) == 0 {
		command = []string{"sh", "-c"}
		args = []string{"sleep 2147483647"}
	}
	resources, err := directResources(r.cfg.Direct)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	return corev1.PodSpec{
		RuntimeClassName:             &runtimeClassName,
		AutomountServiceAccountToken: &falseValue,
		RestartPolicy:                corev1.RestartPolicyAlways,
		NodeSelector: map[string]string{
			"sandbox.gke.io/runtime": "gvisor",
		},
		Tolerations: []corev1.Toleration{{
			Key:      "sandbox.gke.io/runtime",
			Value:    "gvisor",
			Effect:   corev1.TaintEffectNoSchedule,
			Operator: corev1.TolerationOpEqual,
		}},
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: &trueValue,
			RunAsUser:    runAsUser,
		},
		Containers: []corev1.Container{{
			Name:            r.cfg.Container,
			Image:           image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         command,
			Args:            args,
			Ports: []corev1.ContainerPort{{
				Name:          "plugin-grpc",
				ContainerPort: int32(r.cfg.PluginPort),
				Protocol:      corev1.ProtocolTCP,
			}},
			Resources: resources,
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: &falseValue,
				Capabilities: &corev1.Capabilities{
					Drop: []corev1.Capability{"ALL"},
				},
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{"sh", "-c", "test -d /tmp"},
					},
				},
				InitialDelaySeconds: 0,
				PeriodSeconds:       1,
				FailureThreshold:    30,
				TimeoutSeconds:      1,
			},
		}},
	}, nil
}

func directResources(cfg DirectConfig) (corev1.ResourceRequirements, error) {
	requestCPU, err := resource.ParseQuantity(cfg.CPURequest)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("parse direct.cpuRequest: %w", err)
	}
	requestMemory, err := resource.ParseQuantity(cfg.MemoryRequest)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("parse direct.memoryRequest: %w", err)
	}
	limitCPU, err := resource.ParseQuantity(cfg.CPULimit)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("parse direct.cpuLimit: %w", err)
	}
	limitMemory, err := resource.ParseQuantity(cfg.MemoryLimit)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("parse direct.memoryLimit: %w", err)
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    requestCPU,
			corev1.ResourceMemory: requestMemory,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    limitCPU,
			corev1.ResourceMemory: limitMemory,
		},
	}, nil
}

func (r *kubernetesSandboxRuntime) Get(ctx context.Context, handle sandboxHandle) (sandboxHandle, error) {
	if handle.SandboxName == "" && handle.ClaimName != "" {
		claim, err := r.extensions.ExtensionsV1alpha1().SandboxClaims(handle.Namespace).Get(ctx, handle.ClaimName, metav1.GetOptions{})
		if err != nil {
			return sandboxHandle{}, err
		}
		sandboxName, err := r.sandboxNameForClaim(ctx, handle, claim)
		if err != nil {
			return sandboxHandle{}, err
		}
		handle.SandboxName = sandboxName
	}
	return r.refreshSandbox(ctx, handle)
}

func (r *kubernetesSandboxRuntime) Stop(ctx context.Context, handle sandboxHandle) error {
	switch handle.Mode {
	case "claim":
		err := r.extensions.ExtensionsV1alpha1().SandboxClaims(handle.Namespace).Delete(ctx, handle.ClaimName, metav1.DeleteOptions{})
		if k8serrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("delete SandboxClaim %s/%s: %w", handle.Namespace, handle.ClaimName, err)
		}
		return nil
	default:
		err := r.agents.AgentsV1alpha1().Sandboxes(handle.Namespace).Delete(ctx, handle.SandboxName, metav1.DeleteOptions{})
		if k8serrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("delete Sandbox %s/%s: %w", handle.Namespace, handle.SandboxName, err)
		}
		return nil
	}
}

func (r *kubernetesSandboxRuntime) ResolveSession(ctx context.Context, namespace, sessionID string) (sandboxSession, error) {
	namespace = strings.TrimSpace(namespace)
	sessionID = strings.TrimSpace(sessionID)
	if namespace == "" {
		return sandboxSession{}, fmt.Errorf("gke agent sandbox namespace is required")
	}
	if sessionID == "" {
		return sandboxSession{}, fmt.Errorf("plugin runtime session id is required")
	}

	claim, claimErr := r.extensions.ExtensionsV1alpha1().SandboxClaims(namespace).Get(ctx, sessionID, metav1.GetOptions{})
	if claimErr == nil {
		return r.sessionFromClaim(ctx, claim)
	}
	if claimErr != nil && !k8serrors.IsNotFound(claimErr) {
		return sandboxSession{}, fmt.Errorf("get SandboxClaim %s/%s: %w", namespace, sessionID, claimErr)
	}

	sandbox, sandboxErr := r.agents.AgentsV1alpha1().Sandboxes(namespace).Get(ctx, sessionID, metav1.GetOptions{})
	if sandboxErr == nil {
		return r.sessionFromSandbox(ctx, sandbox)
	}
	if sandboxErr != nil && !k8serrors.IsNotFound(sandboxErr) {
		return sandboxSession{}, fmt.Errorf("get Sandbox %s/%s: %w", namespace, sessionID, sandboxErr)
	}

	labelValue := sanitizeLabelValue(sessionID)
	if labelValue != "" {
		selector := labels.Set{runtimeSessionLabel: labelValue}.String()
		claims, err := r.extensions.ExtensionsV1alpha1().SandboxClaims(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return sandboxSession{}, fmt.Errorf("list SandboxClaims for session %q: %w", sessionID, err)
		}
		matches := make([]sandboxSession, 0, len(claims.Items))
		claimedSandboxes := map[string]struct{}{}
		for i := range claims.Items {
			session, err := r.sessionFromClaim(ctx, &claims.Items[i])
			if err != nil {
				return sandboxSession{}, err
			}
			if session.Handle.SandboxName != "" {
				claimedSandboxes[session.Handle.SandboxName] = struct{}{}
			}
			matches = append(matches, session)
		}
		sandboxes, err := r.agents.AgentsV1alpha1().Sandboxes(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return sandboxSession{}, fmt.Errorf("list Sandboxes for session %q: %w", sessionID, err)
		}
		for i := range sandboxes.Items {
			if _, ok := claimedSandboxes[sandboxes.Items[i].Name]; ok {
				continue
			}
			if len(sandboxes.Items[i].OwnerReferences) > 0 {
				continue
			}
			session, err := r.sessionFromSandbox(ctx, &sandboxes.Items[i])
			if err != nil {
				return sandboxSession{}, err
			}
			matches = append(matches, session)
		}
		switch len(matches) {
		case 0:
		case 1:
			return matches[0], nil
		default:
			return sandboxSession{}, fmt.Errorf("plugin runtime session %q is ambiguous: %d Kubernetes runtime objects matched", sessionID, len(matches))
		}
	}

	return sandboxSession{}, fmt.Errorf("plugin runtime session %q not found", sessionID)
}

// errListSessionsStragglerCap is returned when ListSessions is forced to do
// more straggler GETs (cache misses on referenced Sandboxes) than the cap
// allows. Hitting the cap suggests a structural fanout regression rather than
// the natural single-RV race the fallback is meant to absorb.
var errListSessionsStragglerCap = errors.New("plugin runtime list sessions: straggler cap exceeded")

// listSessionsStragglerCap bounds per-call straggler Get fanout for sessions
// whose backing Sandbox was missing from the namespace LIST. The natural race
// only ever explains at most a handful of misses; a higher count is treated
// as a structural bug.
const listSessionsStragglerCap = 10

// listSessionsCache holds the namespace-wide objects fetched by
// buildListSessionsCache so that per-session enrichment can serve from memory
// instead of re-Getting per-session. Pods carrying a non-nil DeletionTimestamp
// are excluded to match transport refresh and metadata enrichment.
type listSessionsCache struct {
	sandboxesByName     map[string]*sandboxv1alpha1.Sandbox
	podsByName          map[string]*corev1.Pod
	leasesByName        map[string]*coordinationv1.Lease
	sandboxesByClaimUID map[types.UID]string
	templatesByName     map[string]*extv1alpha1.SandboxTemplate
	stragglerBudget     int
}

func newListSessionsCache() *listSessionsCache {
	return &listSessionsCache{
		sandboxesByName:     map[string]*sandboxv1alpha1.Sandbox{},
		podsByName:          map[string]*corev1.Pod{},
		leasesByName:        map[string]*coordinationv1.Lease{},
		sandboxesByClaimUID: map[types.UID]string{},
		templatesByName:     map[string]*extv1alpha1.SandboxTemplate{},
		stragglerBudget:     listSessionsStragglerCap,
	}
}

func (c *listSessionsCache) consumeStraggler() bool {
	if c == nil {
		return false
	}
	if c.stragglerBudget <= 0 {
		return false
	}
	c.stragglerBudget--
	return true
}

func (r *kubernetesSandboxRuntime) buildListSessionsCache(ctx context.Context, namespace string) (*listSessionsCache, *extv1alpha1.SandboxClaimList, *sandboxv1alpha1.SandboxList, error) {
	managedSelector := labels.Set{
		"app.kubernetes.io/managed-by": "gestalt",
		"gestalt.dev/runtime":          "gke-agent-sandbox",
	}.String()
	claims, err := r.extensions.ExtensionsV1alpha1().SandboxClaims(namespace).List(ctx, metav1.ListOptions{LabelSelector: managedSelector})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list SandboxClaims: %w", err)
	}
	sandboxes, err := r.agents.AgentsV1alpha1().Sandboxes(namespace).List(ctx, metav1.ListOptions{LabelSelector: managedSelector})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list Sandboxes: %w", err)
	}
	pods, err := r.core.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list Pods: %w", err)
	}
	leases, err := r.core.CoordinationV1().Leases(namespace).List(ctx, metav1.ListOptions{LabelSelector: managedSelector})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list Leases: %w", err)
	}
	// SandboxTemplates do not carry the managed-by label; the dedicated
	// gestalt runtime namespace makes an unfiltered LIST safe and removes the
	// per-distinct-template GET fanout that runtimeTemplateMetadataFromCache
	// would otherwise issue.
	templates, err := r.extensions.ExtensionsV1alpha1().SandboxTemplates(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list SandboxTemplates: %w", err)
	}

	cache := newListSessionsCache()
	for i := range sandboxes.Items {
		sb := &sandboxes.Items[i]
		cache.sandboxesByName[sb.Name] = sb
		for _, owner := range sb.OwnerReferences {
			if owner.UID != "" {
				cache.sandboxesByClaimUID[owner.UID] = sb.Name
			}
		}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		cache.podsByName[pod.Name] = pod
	}
	for i := range leases.Items {
		lease := &leases.Items[i]
		cache.leasesByName[lease.Name] = lease
	}
	for i := range templates.Items {
		tmpl := &templates.Items[i]
		cache.templatesByName[tmpl.Name] = tmpl
	}
	return cache, claims, sandboxes, nil
}

func (r *kubernetesSandboxRuntime) ListSessions(ctx context.Context, namespace string) ([]sandboxSession, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("gke agent sandbox namespace is required")
	}
	cache, claims, sandboxes, err := r.buildListSessionsCache(ctx, namespace)
	if err != nil {
		return nil, err
	}
	sessions := make([]sandboxSession, 0, len(claims.Items))
	claimedSandboxes := map[string]struct{}{}
	for i := range claims.Items {
		session, err := r.sessionFromClaimWithCache(ctx, &claims.Items[i], cache)
		if err != nil {
			return nil, err
		}
		if session.Handle.SandboxName != "" {
			claimedSandboxes[session.Handle.SandboxName] = struct{}{}
		}
		sessions = append(sessions, session)
	}

	for i := range sandboxes.Items {
		if _, ok := claimedSandboxes[sandboxes.Items[i].Name]; ok {
			continue
		}
		if len(sandboxes.Items[i].OwnerReferences) > 0 {
			continue
		}
		session, err := r.sessionFromSandboxWithCache(ctx, &sandboxes.Items[i], cache)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, nil
}

func (r *kubernetesSandboxRuntime) sessionFromClaim(ctx context.Context, claim *extv1alpha1.SandboxClaim) (sandboxSession, error) {
	if claim == nil {
		return sandboxSession{}, fmt.Errorf("SandboxClaim is required")
	}
	handle := sandboxHandle{
		Name:      claim.Name,
		Namespace: claim.Namespace,
		Mode:      "claim",
		ClaimName: claim.Name,
	}
	sandboxName, err := r.sandboxNameForClaim(ctx, handle, claim)
	if err != nil {
		return sandboxSession{}, err
	}
	if sandboxName != "" {
		handle.SandboxName = sandboxName
		if refreshed, err := r.refreshSandbox(ctx, handle); err == nil {
			handle = refreshed
		} else if !k8serrors.IsNotFound(err) {
			return sandboxSession{}, err
		}
	}
	session := sandboxSessionFromRuntimeObject(
		claim.Name,
		pluginNameFromObject(claim.ObjectMeta),
		strings.TrimSpace(claim.Annotations[sessionTemplateAnnotation]),
		nil,
		claim.Annotations,
		handle,
	)
	if err := r.enrichSessionFromClaim(ctx, &session, claim); err != nil {
		return sandboxSession{}, err
	}
	if !session.PluginStarted {
		starting, err := r.pluginStartLeaseActive(ctx, handle)
		if err != nil {
			return sandboxSession{}, err
		}
		session.PluginStarting = starting
	}
	return session, nil
}

func (r *kubernetesSandboxRuntime) sessionFromSandbox(ctx context.Context, sandbox *sandboxv1alpha1.Sandbox) (sandboxSession, error) {
	if sandbox == nil {
		return sandboxSession{}, fmt.Errorf("Sandbox is required")
	}
	handle := sandboxHandle{
		Name:        sandbox.Name,
		Namespace:   sandbox.Namespace,
		Mode:        "sandbox",
		SandboxName: sandbox.Name,
		Ready:       sandboxReadyCondition(sandbox),
		PodName:     sandboxPodName(sandbox),
	}
	if handle.PodName == "" && sandbox.Status.LabelSelector != "" {
		podName, err := r.firstPodNameForSelector(ctx, sandbox.Namespace, sandbox.Status.LabelSelector)
		if err != nil {
			return sandboxSession{}, err
		}
		handle.PodName = podName
	}
	session := sandboxSessionFromRuntimeObject(
		sandbox.Name,
		pluginNameFromObject(sandbox.ObjectMeta),
		strings.TrimSpace(sandbox.Annotations[sessionTemplateAnnotation]),
		nil,
		sandbox.Annotations,
		handle,
	)
	if err := r.enrichSessionFromSandbox(ctx, &session, sandbox); err != nil {
		return sandboxSession{}, err
	}
	if !session.PluginStarted {
		starting, err := r.pluginStartLeaseActive(ctx, handle)
		if err != nil {
			return sandboxSession{}, err
		}
		session.PluginStarting = starting
	}
	return session, nil
}

func (r *kubernetesSandboxRuntime) sandboxNameForClaimFromCache(_ context.Context, handle sandboxHandle, claim *extv1alpha1.SandboxClaim, cache *listSessionsCache) (string, error) {
	if claim != nil {
		if name := strings.TrimSpace(claim.Status.SandboxStatus.Name); name != "" {
			return name, nil
		}
	}
	if claim != nil && claim.UID != "" && cache != nil {
		if name, ok := cache.sandboxesByClaimUID[claim.UID]; ok {
			return name, nil
		}
	}
	if claim != nil {
		if name := strings.TrimSpace(handle.ClaimName); name != "" && cache != nil {
			if _, ok := cache.sandboxesByName[name]; ok {
				return name, nil
			}
		}
	}
	// Pending claim: the cache holds every Sandbox in the namespace, so a miss
	// here means there is no backing Sandbox yet. Returning "" surfaces a
	// pending session and avoids per-claim kube fanout that would re-introduce
	// the listing-time throttling this path is designed to eliminate.
	return "", nil
}

func (r *kubernetesSandboxRuntime) refreshSandboxFromCache(ctx context.Context, handle sandboxHandle, cache *listSessionsCache, fallback bool) (sandboxHandle, error) {
	if handle.SandboxName == "" {
		return sandboxHandle{}, fmt.Errorf("sandbox name is not available")
	}
	if cache != nil {
		if sb, ok := cache.sandboxesByName[handle.SandboxName]; ok {
			handle.Ready = sandboxReadyCondition(sb)
			handle.PodName = sandboxPodName(sb)
			if handle.PodName == "" && sb.Status.LabelSelector != "" {
				if !fallback {
					return handle, nil
				}
				if !cache.consumeStraggler() {
					return sandboxHandle{}, fmt.Errorf("%w: too many straggler Sandbox lookups during ListSessions: structural fanout", errListSessionsStragglerCap)
				}
				podName, err := r.firstPodNameForSelector(ctx, handle.Namespace, sb.Status.LabelSelector)
				if err != nil {
					return sandboxHandle{}, err
				}
				handle.PodName = podName
			}
			return handle, nil
		}
	}
	if !fallback {
		return handle, nil
	}
	if cache != nil && !cache.consumeStraggler() {
		return sandboxHandle{}, fmt.Errorf("%w: too many straggler Sandbox lookups during ListSessions: structural fanout", errListSessionsStragglerCap)
	}
	return r.refreshSandbox(ctx, handle)
}

func (r *kubernetesSandboxRuntime) pluginStartLeaseActiveFromCache(handle sandboxHandle, cache *listSessionsCache) bool {
	if cache == nil {
		return false
	}
	name := pluginStartLeaseName(handle)
	lease, ok := cache.leasesByName[name]
	if !ok {
		return false
	}
	return pluginStartLeaseHeld(lease, time.Now().UTC())
}

func (r *kubernetesSandboxRuntime) sessionFromClaimWithCache(ctx context.Context, claim *extv1alpha1.SandboxClaim, cache *listSessionsCache) (sandboxSession, error) {
	if claim == nil {
		return sandboxSession{}, fmt.Errorf("SandboxClaim is required")
	}
	handle := sandboxHandle{
		Name:      claim.Name,
		Namespace: claim.Namespace,
		Mode:      "claim",
		ClaimName: claim.Name,
	}
	sandboxName, err := r.sandboxNameForClaimFromCache(ctx, handle, claim, cache)
	if err != nil {
		return sandboxSession{}, err
	}
	if sandboxName != "" {
		handle.SandboxName = sandboxName
		refreshed, err := r.refreshSandboxFromCache(ctx, handle, cache, true)
		if err != nil {
			if errors.Is(err, errListSessionsStragglerCap) {
				return sandboxSession{}, err
			}
			if !k8serrors.IsNotFound(err) {
				return sandboxSession{}, err
			}
		} else {
			handle = refreshed
		}
	}
	session := sandboxSessionFromRuntimeObject(
		claim.Name,
		pluginNameFromObject(claim.ObjectMeta),
		strings.TrimSpace(claim.Annotations[sessionTemplateAnnotation]),
		nil,
		claim.Annotations,
		handle,
	)
	if err := r.enrichSessionFromClaimWithCache(ctx, &session, claim, cache); err != nil {
		return sandboxSession{}, err
	}
	if !session.PluginStarted {
		session.PluginStarting = r.pluginStartLeaseActiveFromCache(handle, cache)
	}
	return session, nil
}

func (r *kubernetesSandboxRuntime) sessionFromSandboxWithCache(ctx context.Context, sandbox *sandboxv1alpha1.Sandbox, cache *listSessionsCache) (sandboxSession, error) {
	if sandbox == nil {
		return sandboxSession{}, fmt.Errorf("Sandbox is required")
	}
	handle := sandboxHandle{
		Name:        sandbox.Name,
		Namespace:   sandbox.Namespace,
		Mode:        "sandbox",
		SandboxName: sandbox.Name,
		Ready:       sandboxReadyCondition(sandbox),
		PodName:     sandboxPodName(sandbox),
	}
	if handle.PodName == "" && sandbox.Status.LabelSelector != "" {
		if cache != nil && !cache.consumeStraggler() {
			return sandboxSession{}, fmt.Errorf("%w: too many straggler Sandbox lookups during ListSessions: structural fanout", errListSessionsStragglerCap)
		}
		podName, err := r.firstPodNameForSelector(ctx, sandbox.Namespace, sandbox.Status.LabelSelector)
		if err != nil {
			return sandboxSession{}, err
		}
		handle.PodName = podName
	}
	session := sandboxSessionFromRuntimeObject(
		sandbox.Name,
		pluginNameFromObject(sandbox.ObjectMeta),
		strings.TrimSpace(sandbox.Annotations[sessionTemplateAnnotation]),
		nil,
		sandbox.Annotations,
		handle,
	)
	if err := r.enrichSessionFromSandboxWithCache(ctx, &session, sandbox, cache); err != nil {
		return sandboxSession{}, err
	}
	if !session.PluginStarted {
		session.PluginStarting = r.pluginStartLeaseActiveFromCache(handle, cache)
	}
	return session, nil
}

func (r *kubernetesSandboxRuntime) EnsureHostnameEgressPolicy(ctx context.Context, handle sandboxHandle, cfg hostnameEgressConfig) (string, error) {
	selector, err := r.hostnameEgressSelector(ctx, handle, cfg)
	if err != nil {
		return "", err
	}
	policy, err := r.hostnameEgressPolicy(ctx, handle, selector, cfg.Endpoints)
	if err != nil {
		return "", err
	}
	policies := r.core.NetworkingV1().NetworkPolicies(handle.Namespace)
	if _, err := policies.Create(ctx, policy, metav1.CreateOptions{}); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("create NetworkPolicy %s/%s: %w", handle.Namespace, policy.Name, err)
		}
		existing, getErr := policies.Get(ctx, policy.Name, metav1.GetOptions{})
		if getErr != nil {
			return "", fmt.Errorf("get NetworkPolicy %s/%s: %w", handle.Namespace, policy.Name, getErr)
		}
		existing.Labels = policy.Labels
		existing.Spec = policy.Spec
		if _, err := policies.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return "", fmt.Errorf("update NetworkPolicy %s/%s: %w", handle.Namespace, policy.Name, err)
		}
	}
	return policy.Name, nil
}

func (r *kubernetesSandboxRuntime) DeleteHostnameEgressPolicy(ctx context.Context, handle sandboxHandle, policyName string) error {
	policyName = strings.TrimSpace(policyName)
	if policyName == "" {
		return nil
	}
	err := r.core.NetworkingV1().NetworkPolicies(handle.Namespace).Delete(ctx, policyName, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete NetworkPolicy %s/%s: %w", handle.Namespace, policyName, err)
	}
	return nil
}

var errPluginAlreadyStarted = errors.New("plugin runtime session already has a running plugin")

func (r *kubernetesSandboxRuntime) AcquirePluginStartLease(ctx context.Context, handle sandboxHandle, holder string, ttl time.Duration) error {
	holder = strings.TrimSpace(holder)
	if holder == "" {
		return fmt.Errorf("plugin start lease holder is required")
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	seconds := int32(ttl.Round(time.Second) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	name := pluginStartLeaseName(handle)
	leases := r.core.CoordinationV1().Leases(handle.Namespace)
	now := metav1.MicroTime{Time: time.Now().UTC()}
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: handle.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "gestalt",
				"gestalt.dev/runtime":          "gke-agent-sandbox",
				runtimeSessionLabel:            sanitizeLabelValue(handle.Name),
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &seconds,
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}
	if _, err := leases.Create(ctx, lease, metav1.CreateOptions{}); err == nil {
		return nil
	} else if !k8serrors.IsAlreadyExists(err) {
		return fmt.Errorf("create Lease %s/%s: %w", handle.Namespace, name, err)
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing, err := leases.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		now := metav1.MicroTime{Time: time.Now().UTC()}
		if pluginStartLeaseHeld(existing, now.Time) {
			return errPluginAlreadyStarted
		}
		existing.Spec.HolderIdentity = &holder
		existing.Spec.LeaseDurationSeconds = &seconds
		existing.Spec.AcquireTime = &now
		existing.Spec.RenewTime = &now
		_, err = leases.Update(ctx, existing, metav1.UpdateOptions{})
		return err
	})
}

func (r *kubernetesSandboxRuntime) ReleasePluginStartLease(ctx context.Context, handle sandboxHandle, holder string) error {
	holder = strings.TrimSpace(holder)
	name := pluginStartLeaseName(handle)
	leases := r.core.CoordinationV1().Leases(handle.Namespace)
	existing, err := leases.Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get Lease %s/%s: %w", handle.Namespace, name, err)
	}
	if holder != "" && strings.TrimSpace(derefString(existing.Spec.HolderIdentity)) != holder {
		return nil
	}
	err = leases.Delete(ctx, name, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete Lease %s/%s: %w", handle.Namespace, name, err)
	}
	return nil
}

func (r *kubernetesSandboxRuntime) pluginStartLeaseActive(ctx context.Context, handle sandboxHandle) (bool, error) {
	name := pluginStartLeaseName(handle)
	lease, err := r.core.CoordinationV1().Leases(handle.Namespace).Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get Lease %s/%s: %w", handle.Namespace, name, err)
	}
	return pluginStartLeaseHeld(lease, time.Now().UTC()), nil
}

func (r *kubernetesSandboxRuntime) MarkPluginStarted(ctx context.Context, handle sandboxHandle, marker, appName string) error {
	marker = strings.TrimSpace(marker)
	if marker == "" {
		return fmt.Errorf("plugin start marker is required")
	}
	return r.updateSessionAnnotations(ctx, handle, func(annotations map[string]string) error {
		if existing := strings.TrimSpace(annotations[pluginStartedAnnotation]); existing != "" && existing != marker {
			return errPluginAlreadyStarted
		}
		annotations[pluginStartedAnnotation] = marker
		if appName = strings.TrimSpace(appName); appName != "" {
			annotations[startedPluginAnnotation] = appName
		}
		return nil
	})
}

func (r *kubernetesSandboxRuntime) Exec(ctx context.Context, handle sandboxHandle, command []string, stdin io.Reader) error {
	_, err := r.execOutput(ctx, handle, command, stdin)
	return err
}

func (r *kubernetesSandboxRuntime) execOutput(ctx context.Context, handle sandboxHandle, command []string, stdin io.Reader) (string, error) {
	if len(command) == 0 {
		return "", fmt.Errorf("exec command is required")
	}
	var err error
	handle, err = r.currentTransportHandle(ctx, handle)
	if err != nil {
		return "", err
	}
	podName := strings.TrimSpace(handle.PodName)
	if podName == "" {
		return "", fmt.Errorf("sandbox pod name is not available")
	}
	if _, err := r.activeTransportPod(ctx, handle); err != nil {
		return "", err
	}
	req := r.core.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(handle.Namespace).
		Name(podName).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: r.cfg.Container,
			Command:   command,
			Stdin:     stdin != nil,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(r.restConfig, http.MethodPost, req.URL())
	if err != nil {
		return "", fmt.Errorf("create pod exec executor: %w", err)
	}
	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return "", fmt.Errorf("exec %q: %w: %s", strings.Join(command, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func (r *kubernetesSandboxRuntime) ForwardPort(ctx context.Context, handle sandboxHandle, remotePort int) (tunnel, error) {
	var err error
	handle, err = r.currentTransportHandle(ctx, handle)
	if err != nil {
		return nil, err
	}
	if handle.PodName == "" {
		return nil, fmt.Errorf("sandbox pod name is not available")
	}
	if _, err := r.activeTransportPod(ctx, handle); err != nil {
		return nil, err
	}
	req := r.core.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(handle.Namespace).
		Name(handle.PodName).
		SubResource("portforward")
	transport, upgrader, err := spdy.RoundTripperFor(r.restConfig)
	if err != nil {
		return nil, fmt.Errorf("create port-forward transport: %w", err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, req.URL())
	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	var out, errOut bytes.Buffer
	pf, err := portforward.NewOnAddresses(
		dialer,
		[]string{"127.0.0.1"},
		[]string{fmt.Sprintf("0:%d", remotePort)},
		stopCh,
		readyCh,
		&out,
		&errOut,
	)
	if err != nil {
		close(stopCh)
		return nil, fmt.Errorf("create port-forward: %w", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- pf.ForwardPorts()
	}()
	select {
	case <-readyCh:
	case err := <-errCh:
		close(stopCh)
		return nil, fmt.Errorf("start port-forward: %w: %s", err, strings.TrimSpace(errOut.String()))
	case <-ctx.Done():
		close(stopCh)
		return nil, fmt.Errorf("start port-forward: %w", ctx.Err())
	}
	ports, err := pf.GetPorts()
	if err != nil {
		close(stopCh)
		return nil, fmt.Errorf("read forwarded port: %w", err)
	}
	if len(ports) == 0 {
		close(stopCh)
		return nil, fmt.Errorf("port-forward did not allocate a local port")
	}
	return &portForwardTunnel{
		stopCh: stopCh,
		errCh:  errCh,
		target: tcpDialTarget("127.0.0.1", int(ports[0].Local)),
	}, nil
}

func (r *kubernetesSandboxRuntime) PodIPDialTarget(ctx context.Context, handle sandboxHandle, remotePort int) (tunnel, error) {
	var err error
	handle, err = r.currentTransportHandle(ctx, handle)
	if err != nil {
		return nil, err
	}
	podName := strings.TrimSpace(handle.PodName)
	if podName == "" {
		return nil, fmt.Errorf("sandbox pod name is not available")
	}
	pod, err := r.activeTransportPod(ctx, handle)
	if err != nil {
		return nil, err
	}
	podIP := strings.TrimSpace(pod.Status.PodIP)
	if podIP == "" {
		return nil, fmt.Errorf("sandbox pod %s/%s does not have a pod IP", handle.Namespace, podName)
	}
	return staticTunnel{target: tcpDialTarget(podIP, remotePort)}, nil
}

func (r *kubernetesSandboxRuntime) ServiceDNSDialTarget(ctx context.Context, handle sandboxHandle, remotePort int) (tunnel, error) {
	var err error
	handle, err = r.currentTransportHandle(ctx, handle)
	if err != nil {
		return nil, err
	}
	serviceName := strings.TrimSpace(handle.SandboxName)
	if serviceName == "" {
		return nil, fmt.Errorf("sandbox name is not available")
	}
	host := serviceName + "." + handle.Namespace + ".svc.cluster.local"
	return staticTunnel{target: tcpDialTarget(host, remotePort)}, nil
}

func (r *kubernetesSandboxRuntime) currentTransportHandle(ctx context.Context, handle sandboxHandle) (sandboxHandle, error) {
	if err := ctx.Err(); err != nil {
		return sandboxHandle{}, err
	}
	if strings.TrimSpace(handle.Namespace) == "" {
		return sandboxHandle{}, fmt.Errorf("sandbox namespace is not available")
	}
	if claimName := strings.TrimSpace(handle.ClaimName); claimName != "" {
		claim, err := r.extensions.ExtensionsV1alpha1().SandboxClaims(handle.Namespace).Get(ctx, claimName, metav1.GetOptions{})
		if err != nil {
			return sandboxHandle{}, fmt.Errorf("get SandboxClaim %s/%s: %w", handle.Namespace, claimName, err)
		}
		sandboxName, err := r.sandboxNameForClaim(ctx, handle, claim)
		if err != nil {
			return sandboxHandle{}, err
		}
		if sandboxName == "" {
			return sandboxHandle{}, fmt.Errorf("SandboxClaim %s/%s does not reference a Sandbox", handle.Namespace, claimName)
		}
		handle.SandboxName = sandboxName
	}
	if strings.TrimSpace(handle.SandboxName) == "" {
		return handle, nil
	}
	return r.refreshSandbox(ctx, handle)
}

func (r *kubernetesSandboxRuntime) activeTransportPod(ctx context.Context, handle sandboxHandle) (*corev1.Pod, error) {
	podName := strings.TrimSpace(handle.PodName)
	if podName == "" {
		return nil, fmt.Errorf("sandbox pod name is not available")
	}
	pod, err := r.core.CoreV1().Pods(handle.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get sandbox pod %s/%s: %w", handle.Namespace, podName, err)
	}
	if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return nil, fmt.Errorf("sandbox pod %s/%s is not active", handle.Namespace, podName)
	}
	return pod, nil
}

func (r *kubernetesSandboxRuntime) Close() error {
	return nil
}

func (r *kubernetesSandboxRuntime) cleanupCreatedSandbox(handle sandboxHandle) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), r.cfg.CleanupTimeout)
	defer cancel()
	if err := r.Stop(cleanupCtx, handle); err != nil {
		return fmt.Errorf("cleanup created sandbox: %w", err)
	}
	return nil
}

func (r *kubernetesSandboxRuntime) waitForClaimReady(ctx context.Context, handle sandboxHandle) (sandboxHandle, error) {
	deadline := time.Now().Add(r.cfg.SandboxReadyTimeout)
	for {
		claim, err := r.extensions.ExtensionsV1alpha1().SandboxClaims(handle.Namespace).Get(ctx, handle.ClaimName, metav1.GetOptions{})
		if err != nil {
			return sandboxHandle{}, fmt.Errorf("get SandboxClaim %s/%s: %w", handle.Namespace, handle.ClaimName, err)
		}
		sandboxName, err := r.sandboxNameForClaim(ctx, handle, claim)
		if err != nil {
			return sandboxHandle{}, err
		}
		if sandboxName != "" {
			handle.SandboxName = sandboxName
			return r.waitForSandboxReady(ctx, handle)
		}
		if time.Now().After(deadline) {
			return sandboxHandle{}, fmt.Errorf("SandboxClaim %s/%s did not resolve a Sandbox within %s", handle.Namespace, handle.ClaimName, r.cfg.SandboxReadyTimeout)
		}
		if err := sleepContext(ctx, 250*time.Millisecond); err != nil {
			return sandboxHandle{}, err
		}
	}
}

func (r *kubernetesSandboxRuntime) sandboxNameForClaim(ctx context.Context, handle sandboxHandle, claim *extv1alpha1.SandboxClaim) (string, error) {
	if claim != nil {
		if name := strings.TrimSpace(claim.Status.SandboxStatus.Name); name != "" {
			return name, nil
		}
	}
	if name := strings.TrimSpace(handle.ClaimName); name != "" {
		sb, err := r.agents.AgentsV1alpha1().Sandboxes(handle.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			return sb.Name, nil
		}
		if !k8serrors.IsNotFound(err) {
			return "", fmt.Errorf("get Sandbox %s/%s for claim fallback: %w", handle.Namespace, name, err)
		}
	}
	if claim == nil || claim.UID == "" {
		return "", nil
	}
	sandboxes, err := r.agents.AgentsV1alpha1().Sandboxes(handle.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list Sandboxes for claim %s/%s fallback: %w", handle.Namespace, handle.ClaimName, err)
	}
	for i := range sandboxes.Items {
		for _, owner := range sandboxes.Items[i].OwnerReferences {
			if owner.UID == claim.UID {
				return sandboxes.Items[i].Name, nil
			}
		}
	}
	return "", nil
}

func (r *kubernetesSandboxRuntime) waitForSandboxReady(ctx context.Context, handle sandboxHandle) (sandboxHandle, error) {
	deadline := time.Now().Add(r.cfg.SandboxReadyTimeout)
	for {
		refreshed, err := r.refreshSandbox(ctx, handle)
		if err != nil {
			return sandboxHandle{}, err
		}
		if sandboxIsReady(refreshed) {
			return refreshed, nil
		}
		if time.Now().After(deadline) {
			return sandboxHandle{}, fmt.Errorf("Sandbox %s/%s did not become ready within %s", handle.Namespace, handle.SandboxName, r.cfg.SandboxReadyTimeout)
		}
		if err := sleepContext(ctx, 250*time.Millisecond); err != nil {
			return sandboxHandle{}, err
		}
	}
}

func (r *kubernetesSandboxRuntime) refreshSandbox(ctx context.Context, handle sandboxHandle) (sandboxHandle, error) {
	if handle.SandboxName == "" {
		return sandboxHandle{}, fmt.Errorf("sandbox name is not available")
	}
	sb, err := r.agents.AgentsV1alpha1().Sandboxes(handle.Namespace).Get(ctx, handle.SandboxName, metav1.GetOptions{})
	if err != nil {
		return sandboxHandle{}, fmt.Errorf("get Sandbox %s/%s: %w", handle.Namespace, handle.SandboxName, err)
	}
	handle.Ready = sandboxReadyCondition(sb)
	handle.PodName = sandboxPodName(sb)
	if handle.PodName == "" && sb.Status.LabelSelector != "" {
		podName, err := r.firstPodNameForSelector(ctx, handle.Namespace, sb.Status.LabelSelector)
		if err != nil {
			return sandboxHandle{}, err
		}
		handle.PodName = podName
	}
	return handle, nil
}

func (r *kubernetesSandboxRuntime) firstPodNameForSelector(ctx context.Context, namespace, selector string) (string, error) {
	parsed, err := labels.Parse(selector)
	if err != nil {
		return "", fmt.Errorf("parse Sandbox pod selector: %w", err)
	}
	pods, err := r.core.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: parsed.String()})
	if err != nil {
		return "", fmt.Errorf("list Sandbox pods: %w", err)
	}
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			return pods.Items[i].Name, nil
		}
	}
	return "", nil
}

func sandboxIsReady(handle sandboxHandle) bool {
	return handle.SandboxName != "" && handle.PodName != "" && handle.Ready
}

func sandboxReadyCondition(sb *sandboxv1alpha1.Sandbox) bool {
	for _, condition := range sb.Status.Conditions {
		if condition.Type == string(sandboxv1alpha1.SandboxConditionReady) && condition.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func sandboxPodName(sb *sandboxv1alpha1.Sandbox) string {
	if sb == nil {
		return ""
	}
	if sb.Annotations != nil {
		if name := strings.TrimSpace(sb.Annotations[sandboxv1alpha1.SandboxPodNameAnnotation]); name != "" {
			return name
		}
	}
	return strings.TrimSpace(sb.Name)
}

type runtimeTemplateMetadata struct {
	Template             string
	ExpectedTemplateHash string
	ExpectedImage        string
}

func (m runtimeTemplateMetadata) metadata() map[string]string {
	out := map[string]string{}
	if m.Template != "" {
		out[metadataRuntimeTemplate] = m.Template
	}
	if m.ExpectedTemplateHash != "" {
		out[metadataExpectedTemplateHash] = m.ExpectedTemplateHash
	}
	if m.ExpectedImage != "" {
		out[metadataExpectedImage] = m.ExpectedImage
	}
	return out
}

func (r *kubernetesSandboxRuntime) runtimeTemplateMetadata(ctx context.Context, namespace, templateName string) (runtimeTemplateMetadata, error) {
	templateName = strings.TrimSpace(templateName)
	if templateName == "" {
		return runtimeTemplateMetadata{}, nil
	}
	template, err := r.extensions.ExtensionsV1alpha1().SandboxTemplates(namespace).Get(ctx, templateName, metav1.GetOptions{})
	if err != nil {
		return runtimeTemplateMetadata{}, fmt.Errorf("get SandboxTemplate %s/%s: %w", namespace, templateName, err)
	}
	image := podSpecContainerImage(template.Spec.PodTemplate.Spec, r.cfg.Container)
	if image == "" {
		return runtimeTemplateMetadata{}, fmt.Errorf("SandboxTemplate %s/%s has no runtime container image for container %q", namespace, templateName, r.cfg.Container)
	}
	hash := ""
	if data, err := json.Marshal(template.Spec.PodTemplate); err == nil {
		sum := sha256.Sum256(data)
		hash = hex.EncodeToString(sum[:])
	}
	return runtimeTemplateMetadata{
		Template:             templateName,
		ExpectedTemplateHash: hash,
		ExpectedImage:        image,
	}, nil
}

func (r *kubernetesSandboxRuntime) sandboxClaimWarmPool() *extv1alpha1.WarmPoolPolicy {
	warmPool := strings.TrimSpace(r.cfg.WarmPool)
	if warmPool == "" {
		return nil
	}
	policy := extv1alpha1.WarmPoolPolicy(warmPool)
	return &policy
}

func (r *kubernetesSandboxRuntime) sandboxClaimLifecycle(now time.Time) *extv1alpha1.Lifecycle {
	if r.cfg.SessionTTL <= 0 {
		return nil
	}
	shutdownTime := metav1.NewTime(now.UTC().Add(r.cfg.SessionTTL))
	return &extv1alpha1.Lifecycle{
		ShutdownTime:   &shutdownTime,
		ShutdownPolicy: extv1alpha1.ShutdownPolicyDeleteForeground,
	}
}

func (r *kubernetesSandboxRuntime) VerifySessionCompatible(ctx context.Context, session sandboxSession) error {
	if !r.cfg.EnforceTemplateImageMatch() || strings.TrimSpace(session.Template) == "" {
		return nil
	}
	expected, err := r.runtimeTemplateMetadata(ctx, session.Handle.Namespace, session.Template)
	if err != nil {
		return err
	}
	actualImage, actualImageID, err := r.podRuntimeImage(ctx, session.Handle)
	if err != nil {
		return err
	}
	if actualImage != expected.ExpectedImage {
		return fmt.Errorf("%w %q: SandboxTemplate %s expects image %q, pod %s runs image %q (imageID %q)", errStaleRuntimeSession, session.ID, session.Template, expected.ExpectedImage, session.Handle.PodName, actualImage, actualImageID)
	}
	return nil
}

func (r *kubernetesSandboxRuntime) enrichSessionFromClaim(ctx context.Context, session *sandboxSession, claim *extv1alpha1.SandboxClaim) error {
	if session == nil || claim == nil {
		return nil
	}
	if !claim.CreationTimestamp.IsZero() {
		startedAt := claim.CreationTimestamp.Time.UTC()
		session.StartedAt = &startedAt
	}
	if claim.Spec.Lifecycle != nil && claim.Spec.Lifecycle.ShutdownTime != nil {
		expiresAt := claim.Spec.Lifecycle.ShutdownTime.Time.UTC()
		session.ExpiresAt = &expiresAt
		if drainAt := recommendedDrainAt(expiresAt, r.cfg.SessionDrainBefore); !drainAt.IsZero() {
			session.DrainAt = &drainAt
		}
	}
	return r.enrichSessionRuntimeMetadata(ctx, session)
}

func (r *kubernetesSandboxRuntime) enrichSessionFromSandbox(ctx context.Context, session *sandboxSession, sandbox *sandboxv1alpha1.Sandbox) error {
	if session == nil || sandbox == nil {
		return nil
	}
	if !sandbox.CreationTimestamp.IsZero() {
		startedAt := sandbox.CreationTimestamp.Time.UTC()
		session.StartedAt = &startedAt
	}
	if sandbox.Spec.Lifecycle.ShutdownTime != nil {
		expiresAt := sandbox.Spec.Lifecycle.ShutdownTime.Time.UTC()
		session.ExpiresAt = &expiresAt
		if drainAt := recommendedDrainAt(expiresAt, r.cfg.SessionDrainBefore); !drainAt.IsZero() {
			session.DrainAt = &drainAt
		}
	}
	return r.enrichSessionRuntimeMetadata(ctx, session)
}

func (r *kubernetesSandboxRuntime) enrichSessionRuntimeMetadata(ctx context.Context, session *sandboxSession) error {
	if session == nil {
		return nil
	}
	if session.Metadata == nil {
		session.Metadata = map[string]string{}
	}
	if session.Template != "" {
		template, err := r.runtimeTemplateMetadata(ctx, session.Handle.Namespace, session.Template)
		if err != nil && !k8serrors.IsNotFound(err) {
			return err
		}
		for key, value := range template.metadata() {
			if _, exists := session.Metadata[key]; !exists || key == metadataRuntimeTemplate {
				session.Metadata[key] = value
			}
		}
		if template.ExpectedImage != "" {
			session.Metadata[metadataCurrentImage] = template.ExpectedImage
		}
	}
	actualImage, actualImageID, err := r.podRuntimeImage(ctx, session.Handle)
	if err != nil {
		if k8serrors.IsNotFound(err) || session.Handle.PodName == "" {
			return nil
		}
		return err
	}
	if actualImage != "" {
		session.Metadata[metadataActualImage] = actualImage
	}
	if actualImageID != "" {
		session.Metadata[metadataActualImageID] = actualImageID
	}
	expectedImage := strings.TrimSpace(session.Metadata[metadataCurrentImage])
	if expectedImage == "" {
		expectedImage = strings.TrimSpace(session.Metadata[metadataExpectedImage])
	}
	if expectedImage != "" && actualImage != "" {
		session.Metadata[metadataImageMatch] = strconv.FormatBool(expectedImage == actualImage)
	}
	return nil
}

func (r *kubernetesSandboxRuntime) enrichSessionFromClaimWithCache(ctx context.Context, session *sandboxSession, claim *extv1alpha1.SandboxClaim, cache *listSessionsCache) error {
	if session == nil || claim == nil {
		return nil
	}
	if !claim.CreationTimestamp.IsZero() {
		startedAt := claim.CreationTimestamp.Time.UTC()
		session.StartedAt = &startedAt
	}
	if claim.Spec.Lifecycle != nil && claim.Spec.Lifecycle.ShutdownTime != nil {
		expiresAt := claim.Spec.Lifecycle.ShutdownTime.Time.UTC()
		session.ExpiresAt = &expiresAt
		if drainAt := recommendedDrainAt(expiresAt, r.cfg.SessionDrainBefore); !drainAt.IsZero() {
			session.DrainAt = &drainAt
		}
	}
	return r.enrichSessionRuntimeMetadataFromCache(ctx, session, cache)
}

func (r *kubernetesSandboxRuntime) enrichSessionFromSandboxWithCache(ctx context.Context, session *sandboxSession, sandbox *sandboxv1alpha1.Sandbox, cache *listSessionsCache) error {
	if session == nil || sandbox == nil {
		return nil
	}
	if !sandbox.CreationTimestamp.IsZero() {
		startedAt := sandbox.CreationTimestamp.Time.UTC()
		session.StartedAt = &startedAt
	}
	if sandbox.Spec.Lifecycle.ShutdownTime != nil {
		expiresAt := sandbox.Spec.Lifecycle.ShutdownTime.Time.UTC()
		session.ExpiresAt = &expiresAt
		if drainAt := recommendedDrainAt(expiresAt, r.cfg.SessionDrainBefore); !drainAt.IsZero() {
			session.DrainAt = &drainAt
		}
	}
	return r.enrichSessionRuntimeMetadataFromCache(ctx, session, cache)
}

func (r *kubernetesSandboxRuntime) runtimeTemplateMetadataFromCache(_ context.Context, namespace, templateName string, cache *listSessionsCache) (runtimeTemplateMetadata, error) {
	templateName = strings.TrimSpace(templateName)
	if templateName == "" {
		return runtimeTemplateMetadata{}, nil
	}
	if cache == nil {
		return runtimeTemplateMetadata{}, nil
	}
	template, ok := cache.templatesByName[templateName]
	if !ok {
		// Templates are LISTed up-front in buildListSessionsCache; a miss here
		// means the template was deleted or has not propagated yet. The bulk
		// listing path skips enrichment silently rather than issuing a GET
		// per distinct template, which would re-introduce per-session fanout.
		return runtimeTemplateMetadata{}, nil
	}
	image := podSpecContainerImage(template.Spec.PodTemplate.Spec, r.cfg.Container)
	if image == "" {
		return runtimeTemplateMetadata{}, fmt.Errorf("SandboxTemplate %s/%s has no runtime container image for container %q", namespace, templateName, r.cfg.Container)
	}
	hash := ""
	if data, err := json.Marshal(template.Spec.PodTemplate); err == nil {
		sum := sha256.Sum256(data)
		hash = hex.EncodeToString(sum[:])
	}
	return runtimeTemplateMetadata{
		Template:             templateName,
		ExpectedTemplateHash: hash,
		ExpectedImage:        image,
	}, nil
}

func (r *kubernetesSandboxRuntime) podRuntimeImageFromCache(handle sandboxHandle, cache *listSessionsCache) (string, string, bool) {
	if cache == nil {
		return "", "", false
	}
	podName := strings.TrimSpace(handle.PodName)
	if podName == "" {
		return "", "", false
	}
	pod, ok := cache.podsByName[podName]
	if !ok {
		return "", "", false
	}
	return podSpecContainerImage(pod.Spec, r.cfg.Container), podStatusContainerImageID(pod.Status, r.cfg.Container), true
}

func (r *kubernetesSandboxRuntime) enrichSessionRuntimeMetadataFromCache(ctx context.Context, session *sandboxSession, cache *listSessionsCache) error {
	if session == nil {
		return nil
	}
	if session.Metadata == nil {
		session.Metadata = map[string]string{}
	}
	if session.Template != "" {
		template, err := r.runtimeTemplateMetadataFromCache(ctx, session.Handle.Namespace, session.Template, cache)
		if err != nil && !k8serrors.IsNotFound(err) {
			return err
		}
		for key, value := range template.metadata() {
			if _, exists := session.Metadata[key]; !exists || key == metadataRuntimeTemplate {
				session.Metadata[key] = value
			}
		}
		if template.ExpectedImage != "" {
			session.Metadata[metadataCurrentImage] = template.ExpectedImage
		}
	}
	actualImage, actualImageID, ok := r.podRuntimeImageFromCache(session.Handle, cache)
	if !ok {
		if session.Handle.PodName == "" {
			return nil
		}
		return nil
	}
	if actualImage != "" {
		session.Metadata[metadataActualImage] = actualImage
	}
	if actualImageID != "" {
		session.Metadata[metadataActualImageID] = actualImageID
	}
	expectedImage := strings.TrimSpace(session.Metadata[metadataCurrentImage])
	if expectedImage == "" {
		expectedImage = strings.TrimSpace(session.Metadata[metadataExpectedImage])
	}
	if expectedImage != "" && actualImage != "" {
		session.Metadata[metadataImageMatch] = strconv.FormatBool(expectedImage == actualImage)
	}
	return nil
}

func (r *kubernetesSandboxRuntime) podRuntimeImage(ctx context.Context, handle sandboxHandle) (string, string, error) {
	if strings.TrimSpace(handle.PodName) == "" {
		return "", "", fmt.Errorf("plugin runtime session %q has no backing pod", handle.Name)
	}
	pod, err := r.core.CoreV1().Pods(handle.Namespace).Get(ctx, handle.PodName, metav1.GetOptions{})
	if err != nil {
		return "", "", fmt.Errorf("get Pod %s/%s: %w", handle.Namespace, handle.PodName, err)
	}
	return podSpecContainerImage(pod.Spec, r.cfg.Container), podStatusContainerImageID(pod.Status, r.cfg.Container), nil
}

func podSpecContainerImage(spec corev1.PodSpec, containerName string) string {
	containerName = strings.TrimSpace(containerName)
	for _, container := range spec.Containers {
		if strings.TrimSpace(container.Name) == containerName {
			return strings.TrimSpace(container.Image)
		}
	}
	if len(spec.Containers) == 1 {
		return strings.TrimSpace(spec.Containers[0].Image)
	}
	return ""
}

func podStatusContainerImageID(status corev1.PodStatus, containerName string) string {
	containerName = strings.TrimSpace(containerName)
	for _, container := range status.ContainerStatuses {
		if strings.TrimSpace(container.Name) == containerName {
			return strings.TrimSpace(container.ImageID)
		}
	}
	if len(status.ContainerStatuses) == 1 {
		return strings.TrimSpace(status.ContainerStatuses[0].ImageID)
	}
	return ""
}

func recommendedDrainAt(expiresAt time.Time, drainBefore time.Duration) time.Time {
	if expiresAt.IsZero() || drainBefore <= 0 {
		return time.Time{}
	}
	return expiresAt.Add(-drainBefore).UTC()
}

func mergeRuntimeMetadata(metadata map[string]string, runtimeMetadata map[string]string) map[string]string {
	out := cloneStringMap(metadata)
	if out == nil {
		out = map[string]string{}
	}
	for key, value := range runtimeMetadata {
		if strings.TrimSpace(value) != "" {
			out[key] = value
		}
	}
	return out
}

const (
	runtimeSessionLabel       = "gestalt.dev/runtime-session"
	sessionMetadataAnnotation = "gestalt.dev/session-metadata"
	sessionTemplateAnnotation = "gestalt.dev/session-template"
	sessionPluginAnnotation   = "gestalt.dev/session-plugin"
	pluginStartedAnnotation   = "gestalt.dev/plugin-started"
	startedPluginAnnotation   = "gestalt.dev/started-plugin"

	metadataRuntimeTemplate      = "runtime.template"
	metadataExpectedTemplateHash = "runtime.expectedTemplateHash"
	metadataExpectedImage        = "runtime.expectedImage"
	metadataCurrentImage         = "runtime.currentImage"
	metadataActualImage          = "runtime.actualImage"
	metadataActualImageID        = "runtime.actualImageID"
	metadataImageMatch           = "runtime.imageMatch"
)

func runtimeLabels(appName string) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "gestalt",
		"gestalt.dev/runtime":          "gke-agent-sandbox",
	}
	if value := sanitizeLabelValue(appName); value != "" {
		labels["gestalt.dev/plugin"] = value
	}
	return labels
}

func runtimeObjectMeta(req startSandboxRequest) metav1.ObjectMeta {
	labels := runtimeLabels(req.AppName)
	if sessionLabel := sanitizeLabelValue(req.Name); sessionLabel != "" {
		labels[runtimeSessionLabel] = sessionLabel
	}
	annotations := map[string]string{}
	if appName := strings.TrimSpace(req.AppName); appName != "" {
		annotations[sessionPluginAnnotation] = appName
	}
	if template := strings.TrimSpace(req.Template); template != "" {
		annotations[sessionTemplateAnnotation] = template
	}
	if len(req.Metadata) > 0 {
		if encoded, err := json.Marshal(req.Metadata); err == nil {
			annotations[sessionMetadataAnnotation] = string(encoded)
		}
		for key, value := range req.Metadata {
			if strings.HasPrefix(key, "runtime.") && strings.TrimSpace(value) != "" {
				annotations[key] = value
			}
		}
	}
	return metav1.ObjectMeta{
		Name:        req.Name,
		Namespace:   req.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}
}

func sandboxSessionFromRuntimeObject(id, appName, template string, metadata map[string]string, annotations map[string]string, handle sandboxHandle) sandboxSession {
	if annotations == nil {
		annotations = map[string]string{}
	}
	out := sandboxSession{
		ID:            strings.TrimSpace(id),
		AppName:    strings.TrimSpace(appName),
		Template:      strings.TrimSpace(template),
		Metadata:      cloneStringMap(metadata),
		Handle:        handle,
		PluginStarted: strings.TrimSpace(annotations[pluginStartedAnnotation]) != "",
	}
	if out.Metadata == nil {
		out.Metadata = map[string]string{}
	}
	if out.AppName == "" {
		out.AppName = strings.TrimSpace(annotations[sessionPluginAnnotation])
	}
	if out.Template == "" {
		out.Template = strings.TrimSpace(annotations[sessionTemplateAnnotation])
	}
	if raw := strings.TrimSpace(annotations[sessionMetadataAnnotation]); raw != "" {
		var stored map[string]string
		if err := json.Unmarshal([]byte(raw), &stored); err == nil {
			for key, value := range stored {
				if _, exists := out.Metadata[key]; !exists {
					out.Metadata[key] = value
				}
			}
		}
	}
	if out.ID == "" {
		out.ID = strings.TrimSpace(handle.Name)
	}
	addHandleMetadata(out.Metadata, handle)
	return out
}

func pluginNameFromObject(meta metav1.ObjectMeta) string {
	if name := strings.TrimSpace(meta.Annotations[sessionPluginAnnotation]); name != "" {
		return name
	}
	return strings.TrimSpace(meta.Labels["gestalt.dev/plugin"])
}

func pluginStartLeaseName(handle sandboxHandle) string {
	return dnsLabelWithSuffix(handle.Name, "plugin-start")
}

func pluginStartLeaseHeld(lease *coordinationv1.Lease, now time.Time) bool {
	if lease == nil || strings.TrimSpace(derefString(lease.Spec.HolderIdentity)) == "" {
		return false
	}
	durationSeconds := int32(0)
	if lease.Spec.LeaseDurationSeconds != nil {
		durationSeconds = *lease.Spec.LeaseDurationSeconds
	}
	if durationSeconds <= 0 {
		return true
	}
	renewedAt := time.Time{}
	if lease.Spec.RenewTime != nil {
		renewedAt = lease.Spec.RenewTime.Time
	} else if lease.Spec.AcquireTime != nil {
		renewedAt = lease.Spec.AcquireTime.Time
	}
	if renewedAt.IsZero() {
		return true
	}
	return now.Before(renewedAt.Add(time.Duration(durationSeconds) * time.Second))
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (r *kubernetesSandboxRuntime) updateSessionAnnotations(ctx context.Context, handle sandboxHandle, update func(map[string]string) error) error {
	if update == nil {
		return nil
	}
	switch handle.Mode {
	case "claim":
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			claim, err := r.extensions.ExtensionsV1alpha1().SandboxClaims(handle.Namespace).Get(ctx, handle.ClaimName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if claim.Annotations == nil {
				claim.Annotations = map[string]string{}
			}
			if err := update(claim.Annotations); err != nil {
				return err
			}
			_, err = r.extensions.ExtensionsV1alpha1().SandboxClaims(handle.Namespace).Update(ctx, claim, metav1.UpdateOptions{})
			return err
		})
	default:
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			sandbox, err := r.agents.AgentsV1alpha1().Sandboxes(handle.Namespace).Get(ctx, handle.SandboxName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if sandbox.Annotations == nil {
				sandbox.Annotations = map[string]string{}
			}
			if err := update(sandbox.Annotations); err != nil {
				return err
			}
			_, err = r.agents.AgentsV1alpha1().Sandboxes(handle.Namespace).Update(ctx, sandbox, metav1.UpdateOptions{})
			return err
		})
	}
}

func addHandleMetadata(metadata map[string]string, handle sandboxHandle) {
	if metadata == nil {
		return
	}
	if handle.Namespace != "" {
		metadata["kubernetes.namespace"] = handle.Namespace
	}
	if handle.SandboxName != "" {
		metadata["kubernetes.sandbox"] = handle.SandboxName
	}
	if handle.ClaimName != "" {
		metadata["kubernetes.sandboxClaim"] = handle.ClaimName
	}
	if handle.PodName != "" {
		metadata["kubernetes.pod"] = handle.PodName
	}
}

func (r *kubernetesSandboxRuntime) hostnameEgressSelector(ctx context.Context, handle sandboxHandle, cfg hostnameEgressConfig) (map[string]string, error) {
	switch handle.Mode {
	case "claim":
		templateName := strings.TrimSpace(cfg.Template)
		if templateName == "" {
			return nil, newHostnameEgressPreconditionError("template-backed hosted hostname egress requires a SandboxTemplate name")
		}
		if err := r.ensureUnmanagedTemplate(ctx, handle.Namespace, templateName); err != nil {
			return nil, err
		}
		claim, err := r.extensions.ExtensionsV1alpha1().SandboxClaims(handle.Namespace).Get(ctx, handle.ClaimName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get SandboxClaim %s/%s: %w", handle.Namespace, handle.ClaimName, err)
		}
		if claim.UID == "" {
			return nil, newHostnameEgressPreconditionError("SandboxClaim %s/%s is missing a UID", handle.Namespace, handle.ClaimName)
		}
		return map[string]string{extv1alpha1.SandboxIDLabel: string(claim.UID)}, nil
	default:
		sessionLabel := sanitizeLabelValue(handle.Name)
		if sessionLabel == "" {
			return nil, newHostnameEgressPreconditionError("sandbox session label is not available")
		}
		return map[string]string{runtimeSessionLabel: sessionLabel}, nil
	}
}

func (r *kubernetesSandboxRuntime) ensureUnmanagedTemplate(ctx context.Context, namespace, templateName string) error {
	template, err := r.extensions.ExtensionsV1alpha1().SandboxTemplates(namespace).Get(ctx, templateName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get SandboxTemplate %s/%s: %w", namespace, templateName, err)
	}
	management := template.Spec.NetworkPolicyManagement
	if management == "" {
		management = extv1alpha1.NetworkPolicyManagementManaged
	}
	if management != extv1alpha1.NetworkPolicyManagementUnmanaged {
		return newHostnameEgressPreconditionError(
			"SandboxTemplate %s/%s must set spec.networkPolicyManagement: Unmanaged to enforce hosted hostname egress",
			namespace,
			templateName,
		)
	}
	return nil
}

func (r *kubernetesSandboxRuntime) hostnameEgressPolicy(ctx context.Context, handle sandboxHandle, selector map[string]string, endpoints []hostnameEgressEndpoint) (*networkingv1.NetworkPolicy, error) {
	dnsPeers, err := r.sandboxDNSPeers(ctx, handle)
	if err != nil {
		return nil, err
	}
	egressRules := []networkingv1.NetworkPolicyEgressRule{
		{
			To: dnsPeers,
			Ports: []networkingv1.NetworkPolicyPort{{
				Protocol: protocolPtr(corev1.ProtocolUDP),
				Port:     int32Ptr(53),
			}},
		},
		{
			To: dnsPeers,
			Ports: []networkingv1.NetworkPolicyPort{{
				Protocol: protocolPtr(corev1.ProtocolTCP),
				Port:     int32Ptr(53),
			}},
		},
	}
	for _, endpoint := range endpoints {
		peers, err := r.hostnameEgressPeers(ctx, endpoint.Host)
		if err != nil {
			return nil, err
		}
		egressRules = append(egressRules, networkingv1.NetworkPolicyEgressRule{
			To: peers,
			Ports: []networkingv1.NetworkPolicyPort{{
				Protocol: protocolPtr(corev1.ProtocolTCP),
				Port:     int32Ptr(endpoint.Port),
			}},
		})
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dnsLabelWithSuffix(handle.Name, "egress"),
			Namespace: handle.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "gestalt",
				"gestalt.dev/runtime":          "gke-agent-sandbox",
			},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: selector},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress:      egressRules,
		},
	}, nil
}

func (r *kubernetesSandboxRuntime) sandboxDNSPeers(ctx context.Context, handle sandboxHandle) ([]networkingv1.NetworkPolicyPeer, error) {
	resolvConf, err := r.readSandboxFile(ctx, handle, "/etc/resolv.conf")
	if err != nil {
		return nil, err
	}
	resolvers, err := parseSandboxNameservers(resolvConf)
	if err != nil {
		return nil, newHostnameEgressPreconditionError("discover sandbox DNS resolvers: %v", err)
	}
	return peersForIPs(resolvers)
}

func (r *kubernetesSandboxRuntime) readSandboxFile(ctx context.Context, handle sandboxHandle, path string) (string, error) {
	if r.readFile != nil {
		return r.readFile(ctx, handle, path)
	}
	script := "while IFS= read -r line || [ -n \"$line\" ]; do printf '%s\\n' \"$line\"; done < " + shellQuote(path)
	output, err := r.execOutput(ctx, handle, []string{"sh", "-c", script}, nil)
	if err != nil {
		return "", fmt.Errorf("read sandbox file %q: %w", path, err)
	}
	return output, nil
}

func (r *kubernetesSandboxRuntime) hostnameEgressPeers(ctx context.Context, host string) ([]networkingv1.NetworkPolicyPeer, error) {
	ips, err := r.resolveHostnameEgressEndpoint(ctx, host)
	if err != nil {
		return nil, err
	}
	return peersForIPs(ips)
}

func (r *kubernetesSandboxRuntime) resolveHostnameEgressEndpoint(ctx context.Context, host string) ([]net.IP, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, newHostnameEgressPreconditionError("hostname egress target is empty")
	}
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	lookup := r.lookupIP
	if lookup == nil {
		lookup = net.DefaultResolver.LookupIPAddr
	}
	resolved, err := lookup(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", host, err)
	}
	ips := make([]net.IP, 0, len(resolved))
	seen := make(map[string]struct{}, len(resolved))
	for _, item := range resolved {
		if item.IP == nil {
			continue
		}
		key := item.IP.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ips = append(ips, item.IP)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("resolve %q: no IP addresses returned", host)
	}
	return ips, nil
}

func parseSandboxNameservers(resolvConf string) ([]net.IP, error) {
	lines := strings.Split(resolvConf, "\n")
	ips := make([]net.IP, 0, len(lines))
	seen := make(map[string]struct{}, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "nameserver" {
			continue
		}
		ip := net.ParseIP(fields[1])
		if ip == nil {
			return nil, fmt.Errorf("invalid nameserver %q", fields[1])
		}
		key := ip.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ips = append(ips, ip)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no nameserver entries found")
	}
	return ips, nil
}

func peersForIPs(ips []net.IP) ([]networkingv1.NetworkPolicyPeer, error) {
	peers := make([]networkingv1.NetworkPolicyPeer, 0, len(ips))
	for _, ip := range ips {
		cidr, err := ipCIDR(ip)
		if err != nil {
			return nil, err
		}
		peers = append(peers, networkingv1.NetworkPolicyPeer{
			IPBlock: &networkingv1.IPBlock{CIDR: cidr},
		})
	}
	return peers, nil
}

func ipCIDR(ip net.IP) (string, error) {
	if ip == nil {
		return "", fmt.Errorf("IP address is nil")
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String() + "/32", nil
	}
	if v6 := ip.To16(); v6 != nil {
		return v6.String() + "/128", nil
	}
	return "", fmt.Errorf("unsupported IP address %q", ip.String())
}

func protocolPtr(value corev1.Protocol) *corev1.Protocol {
	return &value
}

func int32Ptr(value int32) *intstr.IntOrString {
	out := intstr.FromInt32(value)
	return &out
}

func sanitizeLabelValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' {
			b.WriteByte(c)
		} else {
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-_.")
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-_.")
	}
	return out
}

func sandboxResourceName(appName, instanceID, sessionID string) string {
	name := sanitizeDNSLabelValue(appName)
	if name == "" {
		name = "plugin"
	}
	prefix := "gestalt-"
	suffixParts := make([]string, 0, 2)
	if value := sanitizeDNSLabelValue(instanceID); value != "" {
		suffixParts = append(suffixParts, value)
	}
	if value := sanitizeDNSLabelValue(sessionID); value != "" {
		suffixParts = append(suffixParts, value)
	}
	suffix := "-session"
	if len(suffixParts) > 0 {
		suffix = "-" + strings.Join(suffixParts, "-")
	}
	maxNameLen := 63 - len(prefix) - len(suffix)
	if maxNameLen < 1 {
		maxNameLen = 1
	}
	if len(name) > maxNameLen {
		name = strings.Trim(name[:maxNameLen], "-_.")
	}
	if name == "" {
		name = "plugin"
	}
	return strings.Trim(prefix+name+suffix, "-")
}

func dnsLabelWithSuffix(base, suffix string) string {
	base = sanitizeDNSLabelValue(base)
	if base == "" {
		base = "resource"
	}
	suffix = sanitizeDNSLabelValue(suffix)
	if suffix == "" {
		return base
	}
	suffix = "-" + suffix
	maxBaseLen := 63 - len(suffix)
	if maxBaseLen < 1 {
		maxBaseLen = 1
	}
	if len(base) > maxBaseLen {
		base = strings.Trim(base[:maxBaseLen], "-")
	}
	if base == "" {
		base = "resource"
	}
	return strings.Trim(base+suffix, "-")
}

func sanitizeDNSLabelValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' {
			b.WriteByte(c)
		} else {
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	return out
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type portForwardTunnel struct {
	stopCh chan struct{}
	errCh  chan error
	target string
	once   sync.Once
}

type staticTunnel struct {
	target string
}

func (t *portForwardTunnel) DialTarget() string {
	if t == nil {
		return ""
	}
	return t.target
}

func (t *portForwardTunnel) Close() error {
	if t == nil {
		return nil
	}
	t.once.Do(func() {
		close(t.stopCh)
	})
	select {
	case err := <-t.errCh:
		if err != nil && !strings.Contains(err.Error(), "lost connection to pod") {
			return err
		}
		return nil
	case <-time.After(2 * time.Second):
		return nil
	}
}

func (t staticTunnel) DialTarget() string {
	return t.target
}

func (staticTunnel) Close() error {
	return nil
}
