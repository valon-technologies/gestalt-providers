package indexeddb

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Provider) ListRelationships(ctx context.Context, req *ListRelationshipsRequest) (*ListRelationshipsResponse, error) {
	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	objectStore := db.ObjectStore(getStoreNames().relationships)
	var filter *RelationshipFilter
	if req != nil {
		filter = req.Filter
	}
	records, err := relationshipRecordsForFilter(ctx, objectStore, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list relationships: %v", err)
	}

	pageSize := int32(defaultRelationshipPageSize)
	pageToken := ""
	if req != nil {
		if req.PageSize < 0 {
			return nil, status.Error(codes.InvalidArgument, "page size must be non-negative")
		}
		if req.PageSize > 0 {
			pageSize = req.PageSize
		}
		pageToken = strings.TrimSpace(req.PageToken)
	}

	offset, err := parseRelationshipPageToken(pageToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "page token is invalid: %v", err)
	}

	matches := make([]*Relationship, 0, len(records))
	for _, record := range records {
		relationship, err := relationshipFromRecord(record)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode relationship: %v", err)
		}
		if relationshipMatchesFilter(relationship, filter) {
			matches = append(matches, relationship)
		}
	}
	if offset > len(matches) {
		return nil, status.Error(codes.InvalidArgument, "page token is out of range")
	}

	limit := int(pageSize)
	if limit == 0 {
		limit = len(matches)
	}
	end := offset + limit
	if end > len(matches) {
		end = len(matches)
	}
	nextPageToken := ""
	if end < len(matches) {
		nextPageToken = strconv.Itoa(end)
	}

	return &ListRelationshipsResponse{
		Relationships: cloneRelationships(matches[offset:end]),
		NextPageToken: nextPageToken,
	}, nil
}
