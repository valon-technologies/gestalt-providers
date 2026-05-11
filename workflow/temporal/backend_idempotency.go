package temporal

import (
	"context"
	"errors"
	"strings"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *temporalBackend) reserveSignalIdempotency(ctx context.Context, req signalIdempotencyReserveRequest) (*proto.SignalWorkflowRunResponse, error) {
	req.Key = strings.TrimSpace(req.Key)
	req.Fingerprint = strings.TrimSpace(req.Fingerprint)
	if req.Key == "" {
		return nil, nil
	}

	entry, existing, err := b.state.reserveSignalIdempotency(ctx, req, defaultIdempotencyRetention, time.Now().UTC())
	if err != nil {
		var conflict *runIdempotencyConflictError
		if errors.As(err, &conflict) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		if status.Code(err) == codes.Aborted {
			return nil, status.Errorf(codes.Aborted, "reserve workflow signal idempotency: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "reserve workflow signal idempotency: %v", err)
	}
	if existing && entry != nil && entry.Status == "completed" {
		if resp := signalResponseFromPayload(entry.ResponsePayload); resp != nil {
			return resp, nil
		}
	}
	return nil, nil
}

type signalIdempotencyRequest struct {
	Operation   string
	Fingerprint string
	OwnerKey    string
	WorkflowKey string
	RunID       string
	Signal      *proto.WorkflowSignal
}

type signalIdempotencyLedgers struct {
	ownerKey    string
	explicitKey string
	fingerprint string
}

func (b *temporalBackend) reserveSignalIdempotencyLedgers(ctx context.Context, req signalIdempotencyRequest) (*signalIdempotencyLedgers, *proto.SignalWorkflowRunResponse, error) {
	req.Operation = strings.TrimSpace(req.Operation)
	req.Fingerprint = strings.TrimSpace(req.Fingerprint)
	req.OwnerKey = strings.TrimSpace(req.OwnerKey)
	req.WorkflowKey = strings.TrimSpace(req.WorkflowKey)
	req.RunID = strings.TrimSpace(req.RunID)

	ledgers := &signalIdempotencyLedgers{
		ownerKey:    ownerIdempotencyLedgerKey(req.OwnerKey, req.Signal.GetIdempotencyKey()),
		explicitKey: explicitSignalLedgerKey(req.Signal),
		fingerprint: req.Fingerprint,
	}

	var ownerResp *proto.SignalWorkflowRunResponse
	if ledgers.ownerKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:                  ledgers.ownerKey,
			Operation:            req.Operation,
			Fingerprint:          req.Fingerprint,
			OwnerKey:             req.OwnerKey,
			WorkflowKey:          req.WorkflowKey,
			RunID:                req.RunID,
			SignalID:             req.Signal.GetId(),
			AllowPayloadVariance: true,
		})
		if err != nil {
			return nil, nil, err
		}
		ownerResp = resp
	}

	var explicitResp *proto.SignalWorkflowRunResponse
	if ledgers.explicitKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:         ledgers.explicitKey,
			Operation:   "signal-id",
			Fingerprint: req.Fingerprint,
			OwnerKey:    req.OwnerKey,
			WorkflowKey: req.WorkflowKey,
			RunID:       req.RunID,
			SignalID:    req.Signal.GetId(),
		})
		if err != nil {
			return nil, nil, err
		}
		explicitResp = resp
	}

	if explicitResp != nil {
		if ledgers.ownerKey != "" && ownerResp == nil {
			if err := b.completeSignalIdempotency(ctx, ledgers.ownerKey, ledgers.fingerprint, explicitResp, true); err != nil {
				return nil, nil, err
			}
		}
		return ledgers, explicitResp, nil
	}
	if ownerResp != nil {
		if ledgers.explicitKey != "" {
			if err := b.completeSignalIdempotency(ctx, ledgers.explicitKey, ledgers.fingerprint, ownerResp, false); err != nil {
				return nil, nil, err
			}
		}
		return ledgers, ownerResp, nil
	}
	return ledgers, nil, nil
}

func (b *temporalBackend) completeSignalIdempotency(ctx context.Context, key, fingerprint string, resp *proto.SignalWorkflowRunResponse, allowPayloadVariance bool) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	if err := b.state.completeSignalIdempotency(ctx, key, fingerprint, resp, allowPayloadVariance, defaultIdempotencyRetention, time.Now().UTC()); err != nil {
		var conflict *runIdempotencyConflictError
		if errors.As(err, &conflict) {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
		return status.Errorf(codes.Internal, "complete workflow signal idempotency: %v", err)
	}
	return nil
}

func (b *temporalBackend) completeSignalIdempotencyLedgers(ctx context.Context, ledgers *signalIdempotencyLedgers, resp *proto.SignalWorkflowRunResponse) error {
	if ledgers == nil {
		return nil
	}
	if err := b.completeSignalIdempotency(ctx, ledgers.ownerKey, ledgers.fingerprint, resp, true); err != nil {
		return err
	}
	if err := b.completeSignalIdempotency(ctx, ledgers.explicitKey, ledgers.fingerprint, resp, false); err != nil {
		return err
	}
	return nil
}
