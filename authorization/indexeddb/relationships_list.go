package indexeddb

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Provider) ListRelationships(ctx context.Context, req *ListRelationshipsRequest) (*ListRelationshipsResponse, error) {
	pageSize := uint32(defaultRelationshipPageSize)
	after := ""
	var filter *RelationshipFilter
	if req != nil {
		if req.PageSize < 0 {
			return nil, status.Error(codes.InvalidArgument, "page size must be non-negative")
		}
		if req.PageSize > 0 {
			pageSize = uint32(req.PageSize)
		}
		filter = req.Filter
		if token := strings.TrimSpace(req.PageToken); token != "" {
			id, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil || len(id) != len("relationship/")+64 || !strings.HasPrefix(string(id), "relationship/") {
				return nil, status.Error(codes.InvalidArgument, "page token is invalid")
			}
			after = string(id)
		}
	}
	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	store := db.ObjectStore(getStoreNames().relationships)
	var reader relationshipReader = store
	indexName, prefix := relationshipPageIndex(filter)
	if indexName != "" {
		reader = store.Index(indexName)
	}

	response := &ListRelationshipsResponse{Relationships: make([]*Relationship, 0)}
	lastMatch := ""
	count := min(uint32(1000), pageSize+1)
	for {
		var query any
		if indexName != "" {
			lower := append([]any{}, prefix...)
			if after != "" {
				lower = append(lower, after)
			}
			query = indexeddb.Bound(lower, append(append([]any{}, prefix...), []any{}), after != "", false)
		} else if after != "" {
			query = indexeddb.LowerBound(after, true)
		}
		records, err := reader.GetAll(ctx, query, count)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list relationships: %v", err)
		}
		for _, record := range records {
			after = stringField(record, "id")
			if after == "" {
				return nil, status.Error(codes.Internal, "relationship record has no id")
			}
			relationship, err := relationshipFromRecord(record)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "decode relationship: %v", err)
			}
			if !relationshipMatchesFilter(relationship, filter) {
				continue
			}
			if uint32(len(response.Relationships)) == pageSize {
				response.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(lastMatch))
				return response, nil
			}
			response.Relationships = append(response.Relationships, relationship)
			lastMatch = after
		}
		if len(records) < int(count) {
			return response, nil
		}
		// Sparse filters can need more candidates to fill the page or find its
		// successor. Continue in bounded chunks instead of one row at a time.
		count = 1000
	}
}

// Fix every endpoint component before seeking by ID, preserving the same ID
// order for indexed and unfiltered listings across both source-layer encodings.
func relationshipPageIndex(filter *RelationshipFilter) (string, []any) {
	if filter == nil {
		return "", nil
	}
	if r := filter.Resource; r != nil && strings.TrimSpace(r.Type) != "" && strings.TrimSpace(r.Id) != "" {
		return "by_resource_page", []any{strings.TrimSpace(r.Type), strings.TrimSpace(r.Id)}
	}
	if target := filter.Target; target != nil {
		if s := target.Subject; s != nil && strings.TrimSpace(s.Type) != "" && strings.TrimSpace(s.Id) != "" {
			return "by_subject_page", []any{strings.TrimSpace(s.Type), strings.TrimSpace(s.Id)}
		}
		if s := target.SubjectSet; s != nil && s.Resource != nil && strings.TrimSpace(s.Resource.Type) != "" && strings.TrimSpace(s.Resource.Id) != "" && strings.TrimSpace(s.Relation) != "" {
			return "by_subject_set_page", []any{strings.TrimSpace(s.Resource.Type), strings.TrimSpace(s.Resource.Id), strings.TrimSpace(s.Relation)}
		}
	}
	return "", nil
}
