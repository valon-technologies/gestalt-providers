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
