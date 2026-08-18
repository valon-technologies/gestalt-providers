package oidc

import (
	"context"
	"sort"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

// grantOwnerSubjects returns every subject key that may own grants for the
// signed-in user. Gestalt principals use user UUID subjects while OIDC login
// and session grants use user:email; list, get, and revoke must query every
// alias.
func (p *Provider) grantOwnerSubjects(ctx context.Context, primary string) []string {
	seen := map[string]struct{}{}
	add := func(subject string) {
		subject = strings.TrimSpace(subject)
		if subject == "" {
			return
		}
		seen[subject] = struct{}{}
	}

	add(primary)
	call := gestalt.IdentityCallContextFromContext(ctx)
	add(call.CallerSubjectID)
	if call.Introspection != nil && call.Introspection.Active {
		add(call.Introspection.Subject)
	}
	if token := strings.TrimSpace(call.CallerBearerToken); token != "" {
		if resp, err := p.Introspect(ctx, &gestalt.IntrospectRequest{Token: token}); err == nil && resp != nil && resp.Active {
			add(resp.Subject)
		}
	}

	if p.claims != nil {
		pending := keysFromSet(seen)
		for _, subject := range pending {
			record, err := p.claims.get(ctx, subject)
			if err != nil || record == nil {
				continue
			}
			add(record.Subject)
			if email := subjectForVerifiedEmail(record.Email); email != "" {
				add(email)
			}
		}
	}

	return keysFromSet(seen)
}

// canonicalGrantSubject picks the stable storage key for a new API-token grant.
// Prefer the canonical user UUID when gestaltd forwards one; otherwise keep
// the subject_token's stored subject.
func (p *Provider) canonicalGrantSubject(ctx context.Context, fallback string) string {
	for _, subject := range p.grantOwnerSubjects(ctx, fallback) {
		if isCanonicalUserUUIDSubject(subject) {
			return subject
		}
	}
	return p.grantOwnerForIssue(ctx, fallback)
}

func isCanonicalUserUUIDSubject(subject string) bool {
	subject = strings.TrimSpace(subject)
	if !strings.HasPrefix(subject, "user:") {
		return false
	}
	id := strings.TrimPrefix(subject, "user:")
	if id == "" || strings.Contains(id, "@") || strings.Contains(id, "|") {
		return false
	}
	return true
}

func keysFromSet(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func grantSubjectMatches(recordSubject string, allowed []string) bool {
	recordSubject = strings.TrimSpace(recordSubject)
	for _, subject := range allowed {
		if recordSubject == strings.TrimSpace(subject) {
			return true
		}
	}
	return false
}
