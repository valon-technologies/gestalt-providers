package gkeagentsandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	agentclientset "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned"
	extclientset "sigs.k8s.io/agent-sandbox/clients/k8s/extensions/clientset/versioned"
	extv1alpha1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"
)

type sandboxRuntime interface {
	HealthCheck(context.Context) error
	Start(context.Context, startSandboxRequest) (sandboxHandle, error)
	Get(context.Context, sandboxHandle) (sandboxHandle, error)
	Stop(context.Context, sandboxHandle) error
	CopyBundle(context.Context, sandboxHandle, string, string) error
	Exec(context.Context, sandboxHandle, []string, io.Reader) error
	ForwardPort(context.Context, sandboxHandle, int) (tunnel, error)
	EnsureHostnameEgressPolicy(context.Context, sandboxHandle, hostnameEgressConfig) (string, error)
	DeleteHostnameEgressPolicy(context.Context, sandboxHandle, string) error
	Close() error
}

type tunnel interface {
	DialTarget() string
	Close() error
}

type startSandboxRequest struct {
	Name       string
	PluginName string
	Namespace  string
	Template   string
	Image      string
	Metadata   map[string]string
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

func newKubernetesSandboxRuntime(cfg Config) (sandboxRuntime, error) {
	restConfig, err := kubernetesRESTConfig(cfg)
	if err != nil {
		return nil, err
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
	if restConfig, err := rest.InClusterConfig(); err == nil {
		return restConfig, nil
	}
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
}

func (r *kubernetesSandboxRuntime) HealthCheck(ctx context.Context) error {
	_, err := r.core.Discovery().ServerVersion()
	if err != nil {
		return fmt.Errorf("gke agent sandbox runtime: kubernetes discovery failed: %w", err)
	}
	return ctx.Err()
}

func (r *kubernetesSandboxRuntime) Start(ctx context.Context, req startSandboxRequest) (sandboxHandle, error) {
	if strings.TrimSpace(req.Template) != "" {
		return r.startClaim(ctx, req)
	}
	return r.startDirectSandbox(ctx, req)
}

func (r *kubernetesSandboxRuntime) startClaim(ctx context.Context, req startSandboxRequest) (sandboxHandle, error) {
	claim := &extv1alpha1.SandboxClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: extv1alpha1.SchemeGroupVersion.String(),
			Kind:       "SandboxClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: req.Namespace,
			Labels:    runtimeLabels(req.PluginName),
		},
		Spec: extv1alpha1.SandboxClaimSpec{
			TemplateRef: extv1alpha1.SandboxTemplateRef{Name: req.Template},
		},
	}
	if _, err := r.extensions.ExtensionsV1alpha1().SandboxClaims(req.Namespace).Create(ctx, claim, metav1.CreateOptions{}); err != nil {
		return sandboxHandle{}, fmt.Errorf("create SandboxClaim %s/%s: %w", req.Namespace, req.Name, err)
	}
	handle := sandboxHandle{
		Name:      req.Name,
		Namespace: req.Namespace,
		Mode:      "claim",
		ClaimName: req.Name,
	}
	ready, err := r.waitForClaimReady(ctx, handle)
	if err != nil {
		return sandboxHandle{}, errors.Join(err, r.cleanupCreatedSandbox(handle))
	}
	return ready, nil
}

func (r *kubernetesSandboxRuntime) startDirectSandbox(ctx context.Context, req startSandboxRequest) (sandboxHandle, error) {
	if strings.TrimSpace(req.Image) == "" {
		return sandboxHandle{}, fmt.Errorf("image is required for direct Sandbox sessions")
	}
	replicas := int32(1)
	podSpec, err := r.directPodSpec(req.Image)
	if err != nil {
		return sandboxHandle{}, err
	}
	objectLabels := runtimeLabels(req.PluginName)
	podLabels := runtimeLabels(req.PluginName)
	sessionLabel := sanitizeLabelValue(req.Name)
	if sessionLabel != "" {
		objectLabels[runtimeSessionLabel] = sessionLabel
		podLabels[runtimeSessionLabel] = sessionLabel
	}
	sandbox := &sandboxv1alpha1.Sandbox{
		TypeMeta: metav1.TypeMeta{
			APIVersion: sandboxv1alpha1.SchemeGroupVersion.String(),
			Kind:       "Sandbox",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: req.Namespace,
			Labels:    objectLabels,
		},
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
		return sandboxHandle{}, fmt.Errorf("create Sandbox %s/%s: %w", req.Namespace, req.Name, err)
	}
	handle := sandboxHandle{
		Name:        req.Name,
		Namespace:   req.Namespace,
		Mode:        "sandbox",
		SandboxName: req.Name,
	}
	ready, err := r.waitForSandboxReady(ctx, handle)
	if err != nil {
		return sandboxHandle{}, errors.Join(err, r.cleanupCreatedSandbox(handle))
	}
	return ready, nil
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
		handle.SandboxName = claim.Status.SandboxStatus.Name
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

func (r *kubernetesSandboxRuntime) CopyBundle(ctx context.Context, handle sandboxHandle, localDir, remoteDir string) error {
	localDir = strings.TrimSpace(localDir)
	if localDir == "" {
		return nil
	}
	if info, err := os.Stat(localDir); err != nil {
		return fmt.Errorf("stat plugin bundle dir: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("plugin bundle path %q is not a directory", localDir)
	}
	var bundle bytes.Buffer
	if err := writeTarDir(&bundle, localDir); err != nil {
		return err
	}
	command := []string{"sh", "-c", "mkdir -p " + shellQuote(remoteDir) + " && tar -xf - -C " + shellQuote(remoteDir)}
	return r.Exec(ctx, handle, command, bytes.NewReader(bundle.Bytes()))
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

func (r *kubernetesSandboxRuntime) Exec(ctx context.Context, handle sandboxHandle, command []string, stdin io.Reader) error {
	_, err := r.execOutput(ctx, handle, command, stdin)
	return err
}

func (r *kubernetesSandboxRuntime) execOutput(ctx context.Context, handle sandboxHandle, command []string, stdin io.Reader) (string, error) {
	if len(command) == 0 {
		return "", fmt.Errorf("exec command is required")
	}
	podName := strings.TrimSpace(handle.PodName)
	if podName == "" {
		return "", fmt.Errorf("sandbox pod name is not available")
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
	if handle.PodName == "" {
		return nil, fmt.Errorf("sandbox pod name is not available")
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
		if claim.Status.SandboxStatus.Name != "" {
			handle.SandboxName = claim.Status.SandboxStatus.Name
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

const runtimeSessionLabel = "gestalt.dev/runtime-session"

func runtimeLabels(pluginName string) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "gestalt",
		"gestalt.dev/runtime":          "gke-agent-sandbox",
	}
	if value := sanitizeLabelValue(pluginName); value != "" {
		labels["gestalt.dev/plugin"] = value
	}
	return labels
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
			Name:      handle.Name + "-egress",
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

func sandboxResourceName(pluginName, sessionID string) string {
	name := sanitizeDNSLabelValue(pluginName)
	if name == "" {
		name = "plugin"
	}
	prefix := "gestalt-"
	suffix := "-" + sanitizeDNSLabelValue(sessionID)
	if suffix == "-" {
		suffix = "-session"
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
