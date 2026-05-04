package externalcredentials

import (
	"context"
	"errors"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type credentialRefreshStats struct {
	Checked   int
	Refreshed int
	Deleted   int
	Skipped   int
	Errors    int
}

func (p *Provider) runCredentialRefreshOnce(ctx context.Context) credentialRefreshStats {
	p.mu.RLock()
	st := p.store
	targets := append([]credentialRefreshTarget(nil), p.cfg.RefreshTargets...)
	p.mu.RUnlock()
	return p.runCredentialRefreshOnceWith(ctx, st, targets)
}

func (p *Provider) runCredentialRefreshOnceWith(ctx context.Context, st *store, targets []credentialRefreshTarget) credentialRefreshStats {
	stats := credentialRefreshStats{}
	if st == nil || len(targets) == 0 {
		return stats
	}
	targetByConnectionID := make(map[string]credentialRefreshTarget, len(targets))
	targetConnectionIDs := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if target.ConnectionID != "" {
			targetByConnectionID[target.ConnectionID] = target
			targetConnectionIDs[target.ConnectionID] = struct{}{}
		}
	}
	if len(targetByConnectionID) == 0 {
		return stats
	}

	credentials, err := st.listCredentialsForConnectionIDs(ctx, targetConnectionIDs)
	if err != nil {
		stats.Errors++
		return stats
	}
	now := p.now().UTC()
	for _, credential := range credentials {
		if err := ctx.Err(); err != nil {
			stats.Errors++
			return stats
		}
		target, ok := targetByConnectionID[credential.GetConnectionId()]
		if !ok {
			stats.Skipped++
			continue
		}
		stats.Checked++
		if !shouldRefreshCredentialWithin(credential, target.Auth, now, target.RefreshBeforeExpiryDuration) {
			stats.Skipped++
			continue
		}
		req := &gestalt.ResolveExternalCredentialRequest{
			Provider:            target.Provider,
			Connection:          target.Connection,
			ConnectionId:        target.ConnectionID,
			Mode:                "user",
			CredentialSubjectId: credential.GetSubjectId(),
			Instance:            credential.GetInstance(),
			Auth:                target.Auth,
			ConnectionParams:    cloneStringMap(target.ConnectionParams),
		}
		_, err := p.refreshStoredCredentialOnce(ctx, st, req, credential)
		switch {
		case err == nil:
			stats.Refreshed++
		case status.Code(err) == codes.Unauthenticated:
			if _, getErr := st.getCredential(ctx, credential.GetSubjectId(), credential.GetConnectionId(), credential.GetInstance()); errors.Is(getErr, gestalt.ErrExternalCredentialNotFound) {
				stats.Deleted++
			} else {
				stats.Errors++
			}
		default:
			stats.Errors++
		}
	}
	return stats
}
