package gkeagentsandbox

import (
	"sort"
	"strings"
)

type startProcessRequest struct {
	Command    string
	Args       []string
	Env        map[string]string
	Workdir    string
	PluginPort int
	SocketPath string
}

func buildLaunchScript(req startProcessRequest) string {
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("mkdir -p /tmp/gestalt\n")
	b.WriteString("rm -f ")
	b.WriteString(shellQuote(req.SocketPath))
	b.WriteByte('\n')
	b.WriteString("command -v socat >/dev/null 2>&1 || { echo 'socat is required to bridge the Gestalt plugin Unix socket to TCP' >&2; exit 127; }\n")
	b.WriteString("nohup socat ")
	b.WriteString("TCP-LISTEN:")
	b.WriteString(intString(req.PluginPort))
	b.WriteString(",fork,reuseaddr ")
	b.WriteString("UNIX-CONNECT:")
	b.WriteString(shellQuote(req.SocketPath))
	b.WriteString(" >/tmp/gestalt-socket-proxy.log 2>&1 &\n")
	b.WriteString("echo $! >/tmp/gestalt-socket-proxy.pid\n")
	if strings.TrimSpace(req.Workdir) != "" {
		b.WriteString("cd ")
		b.WriteString(shellQuote(req.Workdir))
		b.WriteByte('\n')
	}
	b.WriteString("nohup env")
	for _, key := range sortedEnvKeys(req.Env) {
		if strings.TrimSpace(key) == "" || strings.Contains(key, "=") {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(shellQuote(key + "=" + req.Env[key]))
	}
	b.WriteByte(' ')
	b.WriteString(shellQuote(req.Command))
	for _, arg := range req.Args {
		b.WriteByte(' ')
		b.WriteString(shellQuote(arg))
	}
	b.WriteString(" >/tmp/gestalt-plugin.log 2>&1 &\n")
	b.WriteString("echo $! >/tmp/gestalt-plugin.pid\n")
	return b.String()
}

func pluginHealthCommand() []string {
	return []string{"sh", "-c", "test -s /tmp/gestalt-plugin.pid && kill -0 \"$(cat /tmp/gestalt-plugin.pid)\""}
}

func pluginCleanupCommand() []string {
	return []string{"sh", "-c", "for f in /tmp/gestalt-plugin.pid /tmp/gestalt-socket-proxy.pid; do if test -s \"$f\"; then kill \"$(cat \"$f\")\" >/dev/null 2>&1 || true; rm -f \"$f\"; fi; done; rm -f /tmp/gestalt/plugin.sock"}
}

func sortedEnvKeys(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func intString(value int) string {
	if value <= 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[i:])
}
