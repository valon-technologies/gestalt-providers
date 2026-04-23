package gkeagentsandbox

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type hostnameEgressConfig struct {
	Template  string
	Endpoints []hostnameEgressEndpoint
}

type hostnameEgressEndpoint struct {
	Host string
	Port int32
}

type hostnameEgressPreconditionError struct {
	message string
}

func (e *hostnameEgressPreconditionError) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

func newHostnameEgressPreconditionError(format string, args ...any) error {
	return &hostnameEgressPreconditionError{message: fmt.Sprintf(format, args...)}
}

func hostnameEgressStatus(prefix string, err error) error {
	if err == nil {
		return nil
	}
	var preconditionErr *hostnameEgressPreconditionError
	if errors.As(err, &preconditionErr) {
		if prefix == "" {
			return status.Error(codes.FailedPrecondition, preconditionErr.Error())
		}
		return status.Errorf(codes.FailedPrecondition, "%s: %v", prefix, preconditionErr)
	}
	if prefix == "" {
		return status.Errorf(codes.Internal, "%v", err)
	}
	return status.Errorf(codes.Internal, "%s: %v", prefix, err)
}

func requiresHostnameEgress(req *proto.StartHostedPluginRequest, env map[string]string, bindings []hostServiceBinding) bool {
	if req == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(req.GetDefaultAction()), "deny") {
		return true
	}
	for _, proxyEnv := range []string{"HTTPS_PROXY", "HTTP_PROXY"} {
		if strings.TrimSpace(env[proxyEnv]) != "" {
			return true
		}
	}
	relayHosts := relayHostnameSet(bindings)
	for _, host := range req.GetAllowedHosts() {
		if _, ok := relayHosts[normalizeHostname(host)]; !ok {
			return true
		}
	}
	return false
}

func buildHostnameEgressConfig(env map[string]string, bindings []hostServiceBinding, templateName string) (hostnameEgressConfig, error) {
	config := hostnameEgressConfig{
		Template: strings.TrimSpace(templateName),
	}
	added := make(map[string]struct{})
	proxySet := false
	for _, envName := range []string{"HTTPS_PROXY", "HTTP_PROXY"} {
		raw := strings.TrimSpace(env[envName])
		if raw == "" {
			continue
		}
		endpoint, err := parseHostnameEgressTarget(raw)
		if err != nil {
			return hostnameEgressConfig{}, newHostnameEgressPreconditionError("parse %s: %v", envName, err)
		}
		addHostnameEgressEndpoint(&config, added, endpoint)
		proxySet = true
	}
	if !proxySet {
		return hostnameEgressConfig{}, newHostnameEgressPreconditionError("hostname-based egress requires HTTP_PROXY or HTTPS_PROXY")
	}
	for _, binding := range bindings {
		endpoint, err := parseHostnameEgressTarget(binding.dialTarget)
		if err != nil {
			return hostnameEgressConfig{}, newHostnameEgressPreconditionError("parse relay target for %s: %v", binding.envVar, err)
		}
		addHostnameEgressEndpoint(&config, added, endpoint)
	}
	if len(config.Endpoints) == 0 {
		return hostnameEgressConfig{}, newHostnameEgressPreconditionError("hostname-based egress requires at least one reachable relay target")
	}
	return config, nil
}

func addHostnameEgressEndpoint(config *hostnameEgressConfig, added map[string]struct{}, endpoint hostnameEgressEndpoint) {
	if config == nil || strings.TrimSpace(endpoint.Host) == "" || endpoint.Port <= 0 {
		return
	}
	key := strings.ToLower(strings.TrimSpace(endpoint.Host)) + ":" + strconv.Itoa(int(endpoint.Port))
	if _, ok := added[key]; ok {
		return
	}
	added[key] = struct{}{}
	config.Endpoints = append(config.Endpoints, endpoint)
}

func relayHostnameSet(bindings []hostServiceBinding) map[string]struct{} {
	hosts := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		endpoint, err := parseHostnameEgressTarget(binding.dialTarget)
		if err != nil {
			continue
		}
		if host := normalizeHostname(endpoint.Host); host != "" {
			hosts[host] = struct{}{}
		}
	}
	return hosts
}

func normalizeHostname(host string) string {
	return strings.ToLower(strings.TrimSpace(host))
}

func parseHostnameEgressTarget(raw string) (hostnameEgressEndpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return hostnameEgressEndpoint{}, fmt.Errorf("target is empty")
	}
	switch {
	case strings.HasPrefix(raw, "tcp://"), strings.HasPrefix(raw, "tls://"):
		u, err := url.Parse(raw)
		if err != nil {
			return hostnameEgressEndpoint{}, err
		}
		host := strings.TrimSpace(u.Hostname())
		if host == "" {
			return hostnameEgressEndpoint{}, fmt.Errorf("hostname is required")
		}
		port := portFromString(u.Port())
		if port == 0 {
			return hostnameEgressEndpoint{}, fmt.Errorf("port is required")
		}
		return hostnameEgressEndpoint{Host: host, Port: port}, nil
	case strings.HasPrefix(raw, "http://"), strings.HasPrefix(raw, "https://"):
		u, err := url.Parse(raw)
		if err != nil {
			return hostnameEgressEndpoint{}, err
		}
		host := strings.TrimSpace(u.Hostname())
		if host == "" {
			return hostnameEgressEndpoint{}, fmt.Errorf("hostname is required")
		}
		port := portFromString(u.Port())
		if port == 0 {
			if strings.EqualFold(u.Scheme, "https") {
				port = 443
			} else {
				port = 80
			}
		}
		return hostnameEgressEndpoint{Host: host, Port: port}, nil
	default:
		if host, port, err := net.SplitHostPort(raw); err == nil {
			return hostnameEgressEndpoint{Host: strings.TrimSpace(host), Port: portFromString(port)}, nil
		}
		return hostnameEgressEndpoint{}, fmt.Errorf("unsupported target %q", raw)
	}
}

func portFromString(value string) int32 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	port, err := strconv.Atoi(value)
	if err != nil || port <= 0 || port > 65535 {
		return 0
	}
	return int32(port)
}
