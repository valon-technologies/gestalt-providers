package indexeddb

import "testing"

func TestRunAsFromAnyLegacyObject(t *testing.T) {
	got := runAsFromAny(map[string]any{"id": "service:legacy", "email": "ignored@example.com"})
	if got != "service:legacy" {
		t.Fatalf("runAsFromAny = %q", got)
	}
}

func TestRunAsFromAnyScalar(t *testing.T) {
	got := runAsFromAny("service:scalar")
	if got != "service:scalar" {
		t.Fatalf("runAsFromAny = %q", got)
	}
}

func TestRunAsToSubjectBoundary(t *testing.T) {
	subject := runAsToSubject("service:workflow")
	if subject == nil || subject.ID != "service:workflow" {
		t.Fatalf("subject = %#v", subject)
	}
}
