package google

import "testing"

func TestCloseWithoutConfiguredClient(t *testing.T) {
	t.Parallel()

	var provider Provider
	if err := provider.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
