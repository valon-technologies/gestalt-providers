package temporal

import (
	"encoding/json"
	"testing"
)

func TestRunAsIDMarshalScalar(t *testing.T) {
	data, err := json.Marshal(runAsID("service:workflow-test"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(data) != `"service:workflow-test"` {
		t.Fatalf("marshal = %s", string(data))
	}
}

func TestRunAsIDUnmarshalScalar(t *testing.T) {
	var runAs runAsID
	if err := json.Unmarshal([]byte(`"service:workflow-test"`), &runAs); err != nil {
		t.Fatalf("unmarshal scalar: %v", err)
	}
	if string(runAs) != "service:workflow-test" {
		t.Fatalf("runAs = %q", runAs)
	}
}

func TestRunAsIDUnmarshalLegacyObject(t *testing.T) {
	var runAs runAsID
	if err := json.Unmarshal([]byte(`{"id":"service:legacy","email":"ignored@example.com"}`), &runAs); err != nil {
		t.Fatalf("unmarshal legacy object: %v", err)
	}
	if string(runAs) != "service:legacy" {
		t.Fatalf("runAs = %q", runAs)
	}
}

func TestRunAsFromAnyLegacyNestedSubject(t *testing.T) {
	got := runAsFromAny(map[string]any{
		"subject": map[string]any{"id": "service_account:nightly-sync"},
	})
	if got != "service_account:nightly-sync" {
		t.Fatalf("runAsFromAny = %q", got)
	}
}
