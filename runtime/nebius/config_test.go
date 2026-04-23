package nebius

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDecodeConfigRejectsNumericTimeout(t *testing.T) {
	t.Parallel()

	_, err := decodeConfig(map[string]any{
		"subnetID":           "vpcsubnet-123",
		"pluginReadyTimeout": 30,
	})
	if err == nil || !strings.Contains(err.Error(), "Go duration string") {
		t.Fatalf("decodeConfig error = %v, want Go duration string validation error", err)
	}
}

func TestConfigValidateRejectsInvalidUsername(t *testing.T) {
	t.Parallel()

	cfg := Config{
		SubnetID:            "vpcsubnet-123",
		Platform:            "cpu-d3",
		Preset:              "4vcpu-16gb",
		BootDiskSizeGiB:     30,
		BootDiskType:        "network_ssd",
		BootDiskImageFamily: "ubuntu24.04-driverless",
		Username:            "bad/name",
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "valid Linux username") {
		t.Fatalf("Validate error = %v, want Linux username validation error", err)
	}
}

func TestBuildCloudInitIncludesPinnedHostKeys(t *testing.T) {
	t.Parallel()

	doc, err := buildCloudInit(
		"gestalt",
		"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITest user@test",
		"-----BEGIN OPENSSH PRIVATE KEY-----\nTEST\n-----END OPENSSH PRIVATE KEY-----\n",
		"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIhost host@test",
	)
	if err != nil {
		t.Fatalf("buildCloudInit: %v", err)
	}
	body := strings.TrimPrefix(doc, "#cloud-config\n")
	var parsed map[string]any
	if err := yaml.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}

	sshKeys, ok := parsed["ssh_keys"].(map[string]any)
	if !ok {
		t.Fatalf("ssh_keys missing or wrong type: %#v", parsed["ssh_keys"])
	}
	if got := sshKeys["ed25519_public"]; got != "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIhost host@test" {
		t.Fatalf("ssh_keys.ed25519_public = %#v", got)
	}
}
