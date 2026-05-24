package kubernetes

import (
	"slices"
	"testing"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPaginateSessionIDsPagesAndRejectsInvalidRequests(t *testing.T) {
	sessionIDs := []string{"session-a", "session-b", "session-c"}

	first, next, err := paginateSessionIDs(sessionIDs, gestalt.ListRuntimeSessionsRequest{PageSize: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if !slices.Equal(first, []string{"session-a", "session-b"}) {
		t.Fatalf("first page = %#v, want session-a/session-b", first)
	}
	if next == "" {
		t.Fatalf("first page next token is empty")
	}

	second, next, err := paginateSessionIDs(sessionIDs, gestalt.ListRuntimeSessionsRequest{PageSize: 2, PageToken: next})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if !slices.Equal(second, []string{"session-c"}) {
		t.Fatalf("second page = %#v, want session-c", second)
	}
	if next != "" {
		t.Fatalf("second page next token = %q, want empty", next)
	}

	_, _, err = paginateSessionIDs(sessionIDs, gestalt.ListRuntimeSessionsRequest{PageSize: 1, PageToken: "not-base64"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid token code = %v, want InvalidArgument: %v", status.Code(err), err)
	}
}
