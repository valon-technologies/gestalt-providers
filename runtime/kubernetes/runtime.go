package kubernetes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
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
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/client-go/util/retry"
)

type runtimeBackend interface {
	HealthCheck(context.Context) error
	Start(context.Context, startRuntimeSessionRequest) (runtimeSession, error)
	ResolveSession(context.Context, string, string) (runtimeSession, error)
	ListSessions(context.Context, string) ([]runtimeSession, error)
	Stop(context.Context, runtimeHandle) error
	Exec(context.Context, runtimeHandle, []string, io.Reader) error
	ForwardPort(context.Context, runtimeHandle, int) (tunnel, error)
	PodIPDialTarget(context.Context, runtimeHandle, int) (tunnel, error)
	ServiceDNSDialTarget(context.Context, runtimeHandle, int) (tunnel, error)
	EnsureHostnameEgressPolicy(context.Context, runtimeHandle, hostnameEgressConfig) (string, error)
	DeleteHostnameEgressPolicy(context.Context, runtimeHandle, string) error
	AcquirePluginStartLease(context.Context, runtimeHandle, string, time.Duration) error
	ReleasePluginStartLease(context.Context, runtimeHandle, string) error
	MarkPluginStarted(context.Context, runtimeHandle, string, string) error
	Close() error
}

type tunnel interface {
	DialTarget() string
	Close() error
}

type startRuntimeSessionRequest struct {
	Name             string
	AppName       string
	Namespace        string
	Template         string
	Image            string
	DockerConfigJSON string
	Metadata         map[string]string
}

type runtimeSession struct {
	ID             string
	AppName     string
	Template       string
	Metadata       map[string]string
	Handle         runtimeHandle
	PluginStarting bool
	PluginStarted  bool
	Failed         bool
}

type runtimeHandle struct {
	Name      string
	Namespace string
	PodName   string
	PodIP     string
	Ready     bool
}

type kubernetesRuntime struct {
	cfg        Config
	restConfig *rest.Config
	core       k8sclient.Interface
	lookupIP   func(context.Context, string) ([]net.IPAddr, error)
	readFile   func(context.Context, runtimeHandle, string) (string, error)
}

var errPluginAlreadyStarted = errors.New("plugin runtime session already has a running plugin")

func newKubernetesRuntime(cfg Config) (runtimeBackend, error) {
	restConfig, err := kubernetesRESTConfig(cfg)
	if err != nil {
		return nil, err
	}
	core, err := k8sclient.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("kubernetes runtime: create kubernetes client: %w", err)
	}
	return &kubernetesRuntime{
		cfg:        cfg,
		restConfig: rest.CopyConfig(restConfig),
		core:       core,
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
		return nil, fmt.Errorf("kubernetes runtime: create GKE cluster client: %w", err)
	}
	defer client.Close()

	cluster, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: cfg.clusterResourceName()})
	if err != nil {
		return nil, fmt.Errorf("kubernetes runtime: get GKE cluster %s: %w", cfg.clusterResourceName(), err)
	}
	tokenSource, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("kubernetes runtime: load Google application default credentials: %w", err)
	}
	return gkeRESTConfigFromCluster(cfg, cluster, tokenSource)
}

func gkeRESTConfigFromCluster(cfg GKEConfig, cluster *containerpb.Cluster, tokenSource oauth2.TokenSource) (*rest.Config, error) {
	if cluster == nil {
		return nil, fmt.Errorf("kubernetes runtime: GKE cluster response is empty")
	}
	endpoint := strings.TrimSpace(cluster.GetEndpoint())
	if cfg.Endpoint == gkeEndpointPrivate {
		endpoint = strings.TrimSpace(cluster.GetPrivateClusterConfig().GetPrivateEndpoint())
	}
	if endpoint == "" {
		return nil, fmt.Errorf("kubernetes runtime: GKE cluster %s endpoint %q is empty", cfg.clusterResourceName(), cfg.Endpoint)
	}
	if strings.HasPrefix(endpoint, "http://") {
		return nil, fmt.Errorf("kubernetes runtime: GKE cluster %s endpoint must use https", cfg.clusterResourceName())
	}
	if !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	ca := strings.TrimSpace(cluster.GetMasterAuth().GetClusterCaCertificate())
	if ca == "" {
		return nil, fmt.Errorf("kubernetes runtime: GKE cluster %s CA certificate is empty", cfg.clusterResourceName())
	}
	caData, err := base64.StdEncoding.DecodeString(ca)
	if err != nil {
		return nil, fmt.Errorf("kubernetes runtime: decode GKE cluster %s CA certificate: %w", cfg.clusterResourceName(), err)
	}
	if tokenSource == nil {
		return nil, fmt.Errorf("kubernetes runtime: Google token source is required")
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
			return &oauth2.Transport{Source: tokenSource, Base: base}
		},
	}, nil
}

func (r *kubernetesRuntime) HealthCheck(ctx context.Context) error {
	_, err := r.core.Discovery().ServerVersion()
	if err != nil {
		return fmt.Errorf("kubernetes runtime: kubernetes discovery failed: %w", err)
	}
	return ctx.Err()
}

func (r *kubernetesRuntime) Start(ctx context.Context, req startRuntimeSessionRequest) (runtimeSession, error) {
	pod, err := r.podForSession(ctx, req)
	if err != nil {
		return runtimeSession{}, err
	}
	if req.DockerConfigJSON != "" {
		secret, err := r.imagePullSecret(req)
		if err != nil {
			return runtimeSession{}, err
		}
		if _, err := r.core.CoreV1().Secrets(req.Namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			return runtimeSession{}, fmt.Errorf("create image pull Secret %s/%s: %w", req.Namespace, secret.Name, err)
		}
		pod.Spec.ImagePullSecrets = append(pod.Spec.ImagePullSecrets, corev1.LocalObjectReference{Name: secret.Name})
	}
	if r.cfg.ConnectionMode == connectionModeServiceDNS {
		service := r.runtimeService(req)
		if _, err := r.core.CoreV1().Services(req.Namespace).Create(ctx, service, metav1.CreateOptions{}); err != nil {
			_ = r.cleanupCreatedSession(ctx, runtimeHandle{Name: req.Name, Namespace: req.Namespace})
			return runtimeSession{}, fmt.Errorf("create Service %s/%s: %w", req.Namespace, service.Name, err)
		}
	}
	if _, err := r.core.CoreV1().Pods(req.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		_ = r.cleanupCreatedSession(ctx, runtimeHandle{Name: req.Name, Namespace: req.Namespace})
		return runtimeSession{}, fmt.Errorf("create Pod %s/%s: %w", req.Namespace, req.Name, err)
	}
	handle := runtimeHandle{Name: req.Name, Namespace: req.Namespace, PodName: req.Name}
	ready, err := r.waitForPodReady(ctx, handle)
	if err != nil {
		return runtimeSession{}, errors.Join(err, r.cleanupCreatedSession(context.Background(), handle))
	}
	return runtimeSessionFromRuntimeObject(req.Name, req.AppName, req.Template, req.Metadata, pod.Annotations, ready), nil
}

func (r *kubernetesRuntime) podForSession(ctx context.Context, req startRuntimeSessionRequest) (*corev1.Pod, error) {
	req.Namespace = strings.TrimSpace(req.Namespace)
	req.Name = strings.TrimSpace(req.Name)
	if req.Namespace == "" || req.Name == "" {
		return nil, fmt.Errorf("runtime session namespace and name are required")
	}
	var template *corev1.PodTemplateSpec
	if strings.TrimSpace(req.Template) != "" {
		podTemplate, err := r.core.CoreV1().PodTemplates(req.Namespace).Get(ctx, strings.TrimSpace(req.Template), metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get PodTemplate %s/%s: %w", req.Namespace, req.Template, err)
		}
		template = podTemplate.Template.DeepCopy()
	} else {
		if strings.TrimSpace(req.Image) == "" {
			return nil, fmt.Errorf("image is required for direct Kubernetes runtime sessions")
		}
		template = &corev1.PodTemplateSpec{Spec: r.defaultPodSpec(req.Image)}
	}
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        req.Name,
			Namespace:   req.Namespace,
			Labels:      mergeStringMaps(template.Labels, runtimeLabels(req.AppName, req.Name)),
			Annotations: mergeStringMaps(template.Annotations, runtimeAnnotations(req)),
		},
		Spec: *template.Spec.DeepCopy(),
	}
	if err := r.prepareRuntimeContainer(pod, req.Image); err != nil {
		return nil, err
	}
	return pod, nil
}

func (r *kubernetesRuntime) prepareRuntimeContainer(pod *corev1.Pod, image string) error {
	containerName := strings.TrimSpace(r.cfg.Container)
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name != containerName {
			continue
		}
		if strings.TrimSpace(image) != "" {
			pod.Spec.Containers[i].Image = strings.TrimSpace(image)
		}
		if strings.TrimSpace(pod.Spec.Containers[i].Image) == "" {
			return fmt.Errorf("runtime container %q image is required", containerName)
		}
		return nil
	}
	return fmt.Errorf("runtime container %q not found in PodTemplate", containerName)
}

func (r *kubernetesRuntime) defaultPodSpec(image string) corev1.PodSpec {
	falseValue := false
	trueValue := true
	runAsUser := r.cfg.PodDefaults.RunAsUser
	if runAsUser == nil {
		value := defaultRunAsUser
		runAsUser = &value
	}
	command := append([]string(nil), r.cfg.PodDefaults.Command...)
	args := append([]string(nil), r.cfg.PodDefaults.Args...)
	if len(command) == 0 {
		command = []string{"sh", "-c"}
		args = []string{"sleep 2147483647"}
	}
	spec := corev1.PodSpec{
		AutomountServiceAccountToken: &falseValue,
		RestartPolicy:                corev1.RestartPolicyNever,
		NodeSelector:                 cloneStringMap(r.cfg.PodDefaults.NodeSelector),
		Tolerations:                  podDefaultTolerations(r.cfg.PodDefaults.Tolerations),
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: &trueValue,
			RunAsUser:    runAsUser,
		},
		Containers: []corev1.Container{{
			Name:            r.cfg.Container,
			Image:           strings.TrimSpace(image),
			ImagePullPolicy: corev1.PullPolicy(r.cfg.PodDefaults.ImagePullPolicy),
			Command:         command,
			Args:            args,
			Ports: []corev1.ContainerPort{{
				Name:          "plugin-grpc",
				ContainerPort: int32(r.cfg.PluginPort),
				Protocol:      corev1.ProtocolTCP,
			}},
			Resources: directResources(r.cfg.PodDefaults),
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: &falseValue,
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				RunAsNonRoot:             &trueValue,
				RunAsUser:                runAsUser,
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler:        corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "test -d /tmp"}}},
				PeriodSeconds:       1,
				FailureThreshold:    30,
				TimeoutSeconds:      1,
				InitialDelaySeconds: 0,
			},
		}},
	}
	if r.cfg.PodDefaults.ServiceAccountName != "" {
		spec.ServiceAccountName = r.cfg.PodDefaults.ServiceAccountName
	}
	if r.cfg.PodDefaults.RuntimeClassName != "" {
		runtimeClassName := r.cfg.PodDefaults.RuntimeClassName
		spec.RuntimeClassName = &runtimeClassName
	}
	return spec
}

func directResources(cfg PodDefaultsConfig) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cfg.CPURequest),
			corev1.ResourceMemory: resource.MustParse(cfg.MemoryRequest),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cfg.CPULimit),
			corev1.ResourceMemory: resource.MustParse(cfg.MemoryLimit),
		},
	}
}

func podDefaultTolerations(configs []TolerationConfig) []corev1.Toleration {
	out := make([]corev1.Toleration, 0, len(configs))
	for _, cfg := range configs {
		if cfg.Key == "" && cfg.Value == "" && cfg.Effect == "" {
			continue
		}
		toleration := corev1.Toleration{
			Key:      cfg.Key,
			Operator: corev1.TolerationOperator(cfg.Operator),
			Value:    cfg.Value,
			Effect:   corev1.TaintEffect(cfg.Effect),
		}
		if toleration.Operator == "" {
			toleration.Operator = corev1.TolerationOpEqual
		}
		out = append(out, toleration)
	}
	return out
}

func (r *kubernetesRuntime) imagePullSecret(req startRuntimeSessionRequest) (*corev1.Secret, error) {
	if strings.TrimSpace(req.DockerConfigJSON) == "" {
		return nil, fmt.Errorf("docker config JSON is required")
	}
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      imagePullSecretName(runtimeHandle{Name: req.Name}),
			Namespace: req.Namespace,
			Labels:    runtimeLabels(req.AppName, req.Name),
		},
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(req.DockerConfigJSON)},
	}, nil
}

func (r *kubernetesRuntime) runtimeService(req startRuntimeSessionRequest) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: req.Namespace,
			Labels:    runtimeLabels(req.AppName, req.Name),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: runtimeLabels("", req.Name),
			Ports: []corev1.ServicePort{{
				Name:       "plugin-grpc",
				Protocol:   corev1.ProtocolTCP,
				Port:       int32(r.cfg.PluginPort),
				TargetPort: intstr.FromInt32(int32(r.cfg.PluginPort)),
			}},
		},
	}
}

func (r *kubernetesRuntime) ResolveSession(ctx context.Context, namespace, sessionID string) (runtimeSession, error) {
	namespace = strings.TrimSpace(namespace)
	sessionID = strings.TrimSpace(sessionID)
	if namespace == "" {
		return runtimeSession{}, fmt.Errorf("kubernetes runtime namespace is required")
	}
	if sessionID == "" {
		return runtimeSession{}, fmt.Errorf("plugin runtime session id is required")
	}
	if pod, err := r.core.CoreV1().Pods(namespace).Get(ctx, sessionID, metav1.GetOptions{}); err == nil {
		if !runtimeObjectMatchesSession(pod.Labels, sessionID) {
			return runtimeSession{}, fmt.Errorf("plugin runtime session %q not found", sessionID)
		}
		return r.sessionFromPod(ctx, pod)
	} else if !k8serrors.IsNotFound(err) {
		return runtimeSession{}, fmt.Errorf("get Pod %s/%s: %w", namespace, sessionID, err)
	}
	labelValue := sanitizeLabelValue(sessionID)
	if labelValue != "" {
		pods, err := r.core.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: runtimeSessionSelector(labelValue)})
		if err != nil {
			return runtimeSession{}, fmt.Errorf("list Pods for session %q: %w", sessionID, err)
		}
		switch len(pods.Items) {
		case 0:
		case 1:
			return r.sessionFromPod(ctx, &pods.Items[0])
		default:
			return runtimeSession{}, fmt.Errorf("plugin runtime session %q is ambiguous: %d Kubernetes Pods matched", sessionID, len(pods.Items))
		}
	}
	return runtimeSession{}, fmt.Errorf("plugin runtime session %q not found", sessionID)
}

func (r *kubernetesRuntime) ListSessions(ctx context.Context, namespace string) ([]runtimeSession, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("kubernetes runtime namespace is required")
	}
	managedSelector := labels.Set{
		"app.kubernetes.io/managed-by": "gestalt",
		"gestalt.dev/runtime":          "kubernetes",
	}.String()
	pods, err := r.core.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: managedSelector})
	if err != nil {
		return nil, fmt.Errorf("list Pods: %w", err)
	}
	sessions := make([]runtimeSession, 0, len(pods.Items))
	for i := range pods.Items {
		session, err := r.sessionFromPod(ctx, &pods.Items[i])
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, nil
}

func (r *kubernetesRuntime) Stop(ctx context.Context, handle runtimeHandle) error {
	if strings.TrimSpace(handle.PodName) == "" {
		handle.PodName = handle.Name
	}
	var errs []error
	errs = append(errs, r.deleteManagedNetworkPolicy(ctx, handle, hostnameEgressPolicyName(handle)))
	errs = append(errs, r.deleteManagedLease(ctx, handle, pluginStartLeaseName(handle)))
	errs = append(errs, r.deleteManagedService(ctx, handle, handle.Name))
	errs = append(errs, r.deleteManagedSecret(ctx, handle, imagePullSecretName(handle)))
	errs = append(errs, r.deleteManagedPod(ctx, handle, handle.PodName))
	return errors.Join(errs...)
}

func (r *kubernetesRuntime) deleteManagedNetworkPolicy(ctx context.Context, handle runtimeHandle, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	policies := r.core.NetworkingV1().NetworkPolicies(handle.Namespace)
	policy, err := policies.Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get NetworkPolicy %s/%s: %w", handle.Namespace, name, err)
	}
	if !runtimeObjectMatchesSession(policy.Labels, handle.Name) {
		return nil
	}
	if err := policies.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete NetworkPolicy %s/%s: %w", handle.Namespace, name, err)
	}
	return nil
}

func (r *kubernetesRuntime) deleteManagedLease(ctx context.Context, handle runtimeHandle, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	leases := r.core.CoordinationV1().Leases(handle.Namespace)
	lease, err := leases.Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get Lease %s/%s: %w", handle.Namespace, name, err)
	}
	if !runtimeObjectMatchesSession(lease.Labels, handle.Name) {
		return nil
	}
	if err := leases.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete Lease %s/%s: %w", handle.Namespace, name, err)
	}
	return nil
}

func (r *kubernetesRuntime) deleteManagedService(ctx context.Context, handle runtimeHandle, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	services := r.core.CoreV1().Services(handle.Namespace)
	service, err := services.Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get Service %s/%s: %w", handle.Namespace, name, err)
	}
	if !runtimeObjectMatchesSession(service.Labels, handle.Name) {
		return nil
	}
	if err := services.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete Service %s/%s: %w", handle.Namespace, name, err)
	}
	return nil
}

func (r *kubernetesRuntime) deleteManagedSecret(ctx context.Context, handle runtimeHandle, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	secrets := r.core.CoreV1().Secrets(handle.Namespace)
	secret, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get Secret %s/%s: %w", handle.Namespace, name, err)
	}
	if !runtimeObjectMatchesSession(secret.Labels, handle.Name) {
		return nil
	}
	if err := secrets.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete Secret %s/%s: %w", handle.Namespace, name, err)
	}
	return nil
}

func (r *kubernetesRuntime) deleteManagedPod(ctx context.Context, handle runtimeHandle, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	pods := r.core.CoreV1().Pods(handle.Namespace)
	pod, err := pods.Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get Pod %s/%s: %w", handle.Namespace, name, err)
	}
	if !runtimeObjectMatchesSession(pod.Labels, handle.Name) {
		return nil
	}
	if err := pods.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete Pod %s/%s: %w", handle.Namespace, name, err)
	}
	return nil
}

func (r *kubernetesRuntime) sessionFromPod(ctx context.Context, pod *corev1.Pod) (runtimeSession, error) {
	if pod == nil {
		return runtimeSession{}, fmt.Errorf("Pod is required")
	}
	handle := runtimeHandle{
		Name:      pod.Name,
		Namespace: pod.Namespace,
		PodName:   pod.Name,
		PodIP:     strings.TrimSpace(pod.Status.PodIP),
		Ready:     podReadyCondition(pod),
	}
	session := runtimeSessionFromRuntimeObject(pod.Name, pluginNameFromObject(pod.ObjectMeta), strings.TrimSpace(pod.Annotations[sessionTemplateAnnotation]), nil, pod.Annotations, handle)
	if podTerminalFailed(pod) {
		handle.Ready = false
		session.Handle = handle
		session.Failed = true
	}
	if !session.PluginStarted {
		starting, err := r.pluginStartLeaseActive(ctx, handle)
		if err != nil {
			return runtimeSession{}, err
		}
		session.PluginStarting = starting
	}
	return session, nil
}

func podReadyCondition(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func podTerminalFailed(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
}

func (r *kubernetesRuntime) waitForPodReady(ctx context.Context, handle runtimeHandle) (runtimeHandle, error) {
	deadline := time.Now().Add(r.cfg.SessionReadyTimeout)
	for {
		pod, err := r.core.CoreV1().Pods(handle.Namespace).Get(ctx, handle.PodName, metav1.GetOptions{})
		if err != nil {
			return runtimeHandle{}, fmt.Errorf("get Pod %s/%s: %w", handle.Namespace, handle.PodName, err)
		}
		handle.PodIP = strings.TrimSpace(pod.Status.PodIP)
		handle.Ready = podReadyCondition(pod)
		if handle.Ready {
			return handle, nil
		}
		if podTerminalFailed(pod) {
			return runtimeHandle{}, fmt.Errorf("Pod %s/%s entered terminal phase %s", handle.Namespace, handle.PodName, pod.Status.Phase)
		}
		if time.Now().After(deadline) {
			return runtimeHandle{}, fmt.Errorf("Pod %s/%s did not become ready within %s", handle.Namespace, handle.PodName, r.cfg.SessionReadyTimeout)
		}
		if err := sleepContext(ctx, 250*time.Millisecond); err != nil {
			return runtimeHandle{}, err
		}
	}
}

func (r *kubernetesRuntime) Exec(ctx context.Context, handle runtimeHandle, command []string, stdin io.Reader) error {
	_, err := r.execOutput(ctx, handle, command, stdin)
	return err
}

func (r *kubernetesRuntime) execOutput(ctx context.Context, handle runtimeHandle, command []string, stdin io.Reader) (string, error) {
	if len(command) == 0 {
		return "", fmt.Errorf("exec command is required")
	}
	podName := strings.TrimSpace(handle.PodName)
	if podName == "" {
		return "", fmt.Errorf("runtime pod name is not available")
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

func (r *kubernetesRuntime) ForwardPort(ctx context.Context, handle runtimeHandle, remotePort int) (tunnel, error) {
	if handle.PodName == "" {
		return nil, fmt.Errorf("runtime pod name is not available")
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
	pf, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"}, []string{fmt.Sprintf("0:%d", remotePort)}, stopCh, readyCh, &out, &errOut)
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
	return &portForwardTunnel{stopCh: stopCh, errCh: errCh, target: tcpDialTarget("127.0.0.1", int(ports[0].Local))}, nil
}

func (r *kubernetesRuntime) PodIPDialTarget(ctx context.Context, handle runtimeHandle, remotePort int) (tunnel, error) {
	podName := strings.TrimSpace(handle.PodName)
	if podName == "" {
		return nil, fmt.Errorf("runtime pod name is not available")
	}
	pod, err := r.core.CoreV1().Pods(handle.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get runtime pod %s/%s: %w", handle.Namespace, podName, err)
	}
	podIP := strings.TrimSpace(pod.Status.PodIP)
	if podIP == "" {
		return nil, fmt.Errorf("runtime pod %s/%s does not have a pod IP", handle.Namespace, podName)
	}
	return staticTunnel{target: tcpDialTarget(podIP, remotePort)}, nil
}

func (r *kubernetesRuntime) ServiceDNSDialTarget(ctx context.Context, handle runtimeHandle, remotePort int) (tunnel, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	serviceName := strings.TrimSpace(handle.Name)
	if serviceName == "" {
		return nil, fmt.Errorf("runtime service name is not available")
	}
	host := serviceName + "." + handle.Namespace + ".svc.cluster.local"
	return staticTunnel{target: tcpDialTarget(host, remotePort)}, nil
}

func (r *kubernetesRuntime) EnsureHostnameEgressPolicy(ctx context.Context, handle runtimeHandle, cfg hostnameEgressConfig) (string, error) {
	if r.cfg.HostnameEgress.Mode != hostnameEgressModePublicProxy {
		return "", newHostnameEgressPreconditionError("kubernetes hostname egress requires hostnameEgress.mode: publicProxy")
	}
	selector := runtimeLabels("", handle.Name)
	policy, err := r.hostnameEgressPolicy(ctx, handle, selector, cfg.Endpoints)
	if err != nil {
		return "", err
	}
	policies := r.core.NetworkingV1().NetworkPolicies(handle.Namespace)
	existing, err := policies.Get(ctx, policy.Name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		if _, err := policies.Create(ctx, policy, metav1.CreateOptions{}); err != nil {
			return "", fmt.Errorf("create NetworkPolicy %s/%s: %w", handle.Namespace, policy.Name, err)
		}
		return policy.Name, nil
	}
	if err != nil {
		return "", fmt.Errorf("get NetworkPolicy %s/%s: %w", handle.Namespace, policy.Name, err)
	}
	if !runtimeObjectMatchesSession(existing.Labels, handle.Name) {
		return "", runtimeObjectOwnershipError("NetworkPolicy", handle.Namespace, policy.Name, handle.Name)
	}
	existing.Labels = policy.Labels
	existing.Spec = policy.Spec
	if _, err := policies.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return "", fmt.Errorf("update NetworkPolicy %s/%s: %w", handle.Namespace, policy.Name, err)
	}
	return policy.Name, nil
}

func (r *kubernetesRuntime) DeleteHostnameEgressPolicy(ctx context.Context, handle runtimeHandle, policyName string) error {
	policyName = strings.TrimSpace(policyName)
	if policyName == "" {
		return nil
	}
	return r.deleteManagedNetworkPolicy(ctx, handle, policyName)
}

func (r *kubernetesRuntime) AcquirePluginStartLease(ctx context.Context, handle runtimeHandle, holder string, ttl time.Duration) error {
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
			Labels:    runtimeLabels("", handle.Name),
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
		if !runtimeObjectMatchesSession(existing.Labels, handle.Name) {
			return runtimeObjectOwnershipError("Lease", handle.Namespace, name, handle.Name)
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

func (r *kubernetesRuntime) ReleasePluginStartLease(ctx context.Context, handle runtimeHandle, holder string) error {
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
	if !runtimeObjectMatchesSession(existing.Labels, handle.Name) {
		return nil
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

func (r *kubernetesRuntime) pluginStartLeaseActive(ctx context.Context, handle runtimeHandle) (bool, error) {
	name := pluginStartLeaseName(handle)
	lease, err := r.core.CoordinationV1().Leases(handle.Namespace).Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get Lease %s/%s: %w", handle.Namespace, name, err)
	}
	if !runtimeObjectMatchesSession(lease.Labels, handle.Name) {
		return false, nil
	}
	return pluginStartLeaseHeld(lease, time.Now().UTC()), nil
}

func (r *kubernetesRuntime) MarkPluginStarted(ctx context.Context, handle runtimeHandle, marker, appName string) error {
	marker = strings.TrimSpace(marker)
	if marker == "" {
		return fmt.Errorf("plugin start marker is required")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := r.core.CoreV1().Pods(handle.Namespace).Get(ctx, handle.PodName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !runtimeObjectMatchesSession(pod.Labels, handle.Name) {
			return runtimeObjectOwnershipError("Pod", handle.Namespace, handle.PodName, handle.Name)
		}
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		if existing := strings.TrimSpace(pod.Annotations[pluginStartedAnnotation]); existing != "" && existing != marker {
			return errPluginAlreadyStarted
		}
		pod.Annotations[pluginStartedAnnotation] = marker
		if appName = strings.TrimSpace(appName); appName != "" {
			pod.Annotations[startedPluginAnnotation] = appName
		}
		_, err = r.core.CoreV1().Pods(handle.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
		return err
	})
}

func (r *kubernetesRuntime) Close() error {
	return nil
}

func (r *kubernetesRuntime) cleanupCreatedSession(ctx context.Context, handle runtimeHandle) error {
	if handle.PodName == "" {
		handle.PodName = handle.Name
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, r.cfg.CleanupTimeout)
	defer cancel()
	if err := r.Stop(cleanupCtx, handle); err != nil {
		return fmt.Errorf("cleanup created session: %w", err)
	}
	return nil
}

const (
	runtimeSessionLabel       = "gestalt.dev/runtime-session"
	sessionMetadataAnnotation = "gestalt.dev/session-metadata"
	sessionTemplateAnnotation = "gestalt.dev/session-template"
	sessionPluginAnnotation   = "gestalt.dev/session-plugin"
	pluginStartedAnnotation   = "gestalt.dev/plugin-started"
	startedPluginAnnotation   = "gestalt.dev/started-plugin"
)

func runtimeLabels(appName, sessionName string) map[string]string {
	out := map[string]string{
		"app.kubernetes.io/managed-by": "gestalt",
		"gestalt.dev/runtime":          "kubernetes",
	}
	if value := sanitizeLabelValue(sessionName); value != "" {
		out[runtimeSessionLabel] = value
	}
	if value := sanitizeLabelValue(appName); value != "" {
		out["gestalt.dev/plugin"] = value
	}
	return out
}

func runtimeSessionSelector(sessionLabelValue string) string {
	return labels.Set{
		"app.kubernetes.io/managed-by": "gestalt",
		"gestalt.dev/runtime":          "kubernetes",
		runtimeSessionLabel:            sessionLabelValue,
	}.String()
}

func runtimeObjectMatchesSession(objectLabels map[string]string, sessionID string) bool {
	sessionLabel := sanitizeLabelValue(sessionID)
	return sessionLabel != "" &&
		objectLabels["app.kubernetes.io/managed-by"] == "gestalt" &&
		objectLabels["gestalt.dev/runtime"] == "kubernetes" &&
		objectLabels[runtimeSessionLabel] == sessionLabel
}

func runtimeObjectOwnershipError(kind, namespace, name, sessionID string) error {
	return fmt.Errorf("%s %s/%s is not managed by Kubernetes runtime session %q", kind, namespace, name, sessionID)
}

func runtimeAnnotations(req startRuntimeSessionRequest) map[string]string {
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
	}
	return annotations
}

func runtimeSessionFromRuntimeObject(id, appName, template string, metadata map[string]string, annotations map[string]string, handle runtimeHandle) runtimeSession {
	if annotations == nil {
		annotations = map[string]string{}
	}
	out := runtimeSession{
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

func addHandleMetadata(metadata map[string]string, handle runtimeHandle) {
	if metadata == nil {
		return
	}
	if handle.Namespace != "" {
		metadata["kubernetes.namespace"] = handle.Namespace
	}
	if handle.PodName != "" {
		metadata["kubernetes.pod"] = handle.PodName
	}
	if handle.PodIP != "" {
		metadata["kubernetes.podIP"] = handle.PodIP
	}
}

func (r *kubernetesRuntime) hostnameEgressPolicy(ctx context.Context, handle runtimeHandle, selector map[string]string, endpoints []hostnameEgressEndpoint) (*networkingv1.NetworkPolicy, error) {
	dnsPeers, err := r.podDNSPeers(ctx, handle)
	if err != nil {
		return nil, err
	}
	egressRules := []networkingv1.NetworkPolicyEgressRule{
		{To: dnsPeers, Ports: []networkingv1.NetworkPolicyPort{{Protocol: protocolPtr(corev1.ProtocolUDP), Port: int32Ptr(53)}}},
		{To: dnsPeers, Ports: []networkingv1.NetworkPolicyPort{{Protocol: protocolPtr(corev1.ProtocolTCP), Port: int32Ptr(53)}}},
	}
	for _, endpoint := range endpoints {
		peers, err := r.hostnameEgressPeers(ctx, endpoint.Host)
		if err != nil {
			return nil, err
		}
		egressRules = append(egressRules, networkingv1.NetworkPolicyEgressRule{
			To:    peers,
			Ports: []networkingv1.NetworkPolicyPort{{Protocol: protocolPtr(corev1.ProtocolTCP), Port: int32Ptr(endpoint.Port)}},
		})
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hostnameEgressPolicyName(handle),
			Namespace: handle.Namespace,
			Labels:    runtimeLabels("", handle.Name),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: selector},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress:      egressRules,
		},
	}, nil
}

func (r *kubernetesRuntime) podDNSPeers(ctx context.Context, handle runtimeHandle) ([]networkingv1.NetworkPolicyPeer, error) {
	resolvConf, err := r.readPodFile(ctx, handle, "/etc/resolv.conf")
	if err != nil {
		return nil, err
	}
	resolvers, err := parsePodNameservers(resolvConf)
	if err != nil {
		return nil, err
	}
	peers := make([]networkingv1.NetworkPolicyPeer, 0, len(resolvers))
	for _, resolver := range resolvers {
		cidr, err := ipCIDR(resolver)
		if err != nil {
			return nil, err
		}
		peers = append(peers, networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: cidr}})
	}
	return peers, nil
}

func (r *kubernetesRuntime) readPodFile(ctx context.Context, handle runtimeHandle, path string) (string, error) {
	if r.readFile != nil {
		return r.readFile(ctx, handle, path)
	}
	return r.execOutput(ctx, handle, []string{"sh", "-c", "cat " + shellQuote(path)}, nil)
}

func (r *kubernetesRuntime) hostnameEgressPeers(ctx context.Context, host string) ([]networkingv1.NetworkPolicyPeer, error) {
	ips, err := r.resolveHostnameEgressEndpoint(ctx, host)
	if err != nil {
		return nil, err
	}
	return peersForIPs(ips)
}

func (r *kubernetesRuntime) resolveHostnameEgressEndpoint(ctx context.Context, host string) ([]net.IP, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, fmt.Errorf("hostname is required")
	}
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	lookup := r.lookupIP
	if lookup == nil {
		lookup = net.DefaultResolver.LookupIPAddr
	}
	addrs, err := lookup(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve hostname %q: %w", host, err)
	}
	out := make([]net.IP, 0, len(addrs))
	seen := map[string]struct{}{}
	for _, addr := range addrs {
		if addr.IP == nil {
			continue
		}
		key := addr.IP.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, addr.IP)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("hostname %q did not resolve to any IP addresses", host)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i], out[j]) < 0
	})
	return out, nil
}

func parsePodNameservers(resolvConf string) ([]net.IP, error) {
	var out []net.IP
	seen := map[string]struct{}{}
	for _, line := range strings.Split(resolvConf, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "nameserver" {
			continue
		}
		ip := net.ParseIP(fields[1])
		if ip == nil {
			continue
		}
		key := ip.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, ip)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("pod resolv.conf does not contain nameservers")
	}
	return out, nil
}

func peersForIPs(ips []net.IP) ([]networkingv1.NetworkPolicyPeer, error) {
	peers := make([]networkingv1.NetworkPolicyPeer, 0, len(ips))
	for _, ip := range ips {
		cidr, err := ipCIDR(ip)
		if err != nil {
			return nil, err
		}
		peers = append(peers, networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: cidr}})
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

func pluginStartLeaseName(handle runtimeHandle) string {
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

func hostnameEgressPolicyName(handle runtimeHandle) string {
	return dnsLabelWithSuffix(handle.Name, "egress")
}

func imagePullSecretName(handle runtimeHandle) string {
	return dnsLabelWithSuffix(handle.Name, "pull")
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

func runtimeResourceName(appName, instanceID, sessionID string) string {
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
	fullBase := sanitizeDNSLabelString(base)
	if fullBase == "" {
		fullBase = "resource"
	}
	base = fullBase
	suffix = sanitizeDNSLabelString(suffix)
	if suffix == "" {
		return sanitizeDNSLabelValue(base)
	}
	suffix = "-" + shortDNSLabelHash(fullBase) + "-" + suffix
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

func shortDNSLabelHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:4])
}

func sanitizeDNSLabelValue(value string) string {
	out := sanitizeDNSLabelString(value)
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	return out
}

func sanitizeDNSLabelString(value string) string {
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
	return out
}

func mergeStringMaps(base map[string]string, overlay map[string]string) map[string]string {
	out := cloneStringMap(base)
	if out == nil {
		out = map[string]string{}
	}
	for key, value := range overlay {
		if strings.TrimSpace(key) == "" {
			continue
		}
		out[key] = value
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
		return err
	case <-time.After(500 * time.Millisecond):
		return nil
	}
}

func (t staticTunnel) DialTarget() string {
	return t.target
}

func (t staticTunnel) Close() error {
	return nil
}
