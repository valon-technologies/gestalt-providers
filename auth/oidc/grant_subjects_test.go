package oidc

import (
	"context"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestGrantOwnerSubjectsIncludesEmailAlias(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	ctx := context.Background()

	emailSubject := "user:owner@example.com"
	uuidSubject := "user:7770003b-127b-44dc-b5aa-38c0310e4e23"
	if err := p.claims.upsert(ctx, subjectClaimsRecord{
		Subject: emailSubject,
		Email:   "owner@example.com",
		Name:    "Owner",
	}); err != nil {
		t.Fatalf("upsert claims: %v", err)
	}

	callCtx := gestalt.WithIdentityCallContext(ctx, gestalt.IdentityCallContext{
		CallerSubjectID: uuidSubject,
	})
	subjects := p.grantOwnerSubjects(callCtx, emailSubject)
	if !containsString(subjects, emailSubject) {
		t.Fatalf("grantOwnerSubjects() = %v, want email alias %q", subjects, emailSubject)
	}
	if !containsString(subjects, uuidSubject) {
		t.Fatalf("grantOwnerSubjects() = %v, want uuid %q", subjects, uuidSubject)
	}
}

func TestListGrantsFindsTokensStoredUnderEmailAlias(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	ctx := context.Background()

	emailSubject := "user:owner@example.com"
	uuidSubject := "user:7770003b-127b-44dc-b5aa-38c0310e4e23"
	if err := p.claims.upsert(ctx, subjectClaimsRecord{
		Subject: emailSubject,
		Email:   "owner@example.com",
		Name:    "Owner",
	}); err != nil {
		t.Fatalf("upsert claims: %v", err)
	}

	issued, err := p.grants.issue(ctx, emailSubject, "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err != nil {
		t.Fatalf("issue(api_token) error = %v", err)
	}

	callCtx := gestalt.WithIdentityCallContext(ctx, gestalt.IdentityCallContext{
		CallerSubjectID: uuidSubject,
		Introspection: &gestalt.IntrospectResponse{
			Active:  true,
			Subject: emailSubject,
		},
	})
	listResp, err := p.ListGrants(callCtx, &gestalt.ListGrantsRequest{})
	if err != nil {
		t.Fatalf("ListGrants() error = %v", err)
	}
	if len(listResp.GrantIDs) != 1 || listResp.GrantIDs[0] != issued.grantID {
		t.Fatalf("ListGrants() = %v, want [%q]", listResp.GrantIDs, issued.grantID)
	}
}

func TestCanonicalGrantSubjectPrefersUUID(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	ctx := context.Background()

	emailSubject := "user:owner@example.com"
	uuidSubject := "user:7770003b-127b-44dc-b5aa-38c0310e4e23"
	callCtx := gestalt.WithIdentityCallContext(ctx, gestalt.IdentityCallContext{
		CallerSubjectID: uuidSubject,
		Introspection: &gestalt.IntrospectResponse{
			Active:  true,
			Subject: emailSubject,
		},
	})

	got := p.canonicalGrantSubject(callCtx, emailSubject)
	if got != uuidSubject {
		t.Fatalf("canonicalGrantSubject() = %q, want %q", got, uuidSubject)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
