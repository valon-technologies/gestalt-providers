package gkeagentsandbox

import (
	"encoding/base64"
	"encoding/json"
	"sort"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultListSessionsPageSize = 100
	maxListSessionsPageSize     = 200
)

type listSessionsPageToken struct {
	Version  int    `json:"v"`
	PageSize int    `json:"page_size"`
	LastID   string `json:"last_id,omitempty"`
}

func paginateSessionIDs(sessionIDs []string, req gestalt.ListRuntimeSessionsRequest) ([]string, string, error) {
	pageSize := req.PageSize
	if pageSize < 0 {
		return nil, "", status.Error(codes.InvalidArgument, "page_size must be non-negative")
	}
	if pageSize == 0 {
		pageSize = defaultListSessionsPageSize
	}
	if pageSize > maxListSessionsPageSize {
		pageSize = maxListSessionsPageSize
	}

	lastID := ""
	if req.PageToken != "" {
		token, err := decodeListSessionsPageToken(req.PageToken)
		if err != nil {
			return nil, "", err
		}
		if token.Version != 1 {
			return nil, "", status.Error(codes.InvalidArgument, "page_token has unsupported version")
		}
		if token.PageSize != pageSize {
			return nil, "", status.Error(codes.InvalidArgument, "page_token does not match page_size")
		}
		lastID = token.LastID
	}

	start := 0
	if lastID != "" {
		start = sort.Search(len(sessionIDs), func(i int) bool {
			return sessionIDs[i] > lastID
		})
	}
	if start >= len(sessionIDs) {
		return nil, "", nil
	}
	end := start + pageSize
	if end > len(sessionIDs) {
		end = len(sessionIDs)
	}
	nextPageToken := ""
	if end < len(sessionIDs) {
		var err error
		nextPageToken, err = encodeListSessionsPageToken(listSessionsPageToken{
			Version:  1,
			PageSize: pageSize,
			LastID:   sessionIDs[end-1],
		})
		if err != nil {
			return nil, "", err
		}
	}
	return sessionIDs[start:end], nextPageToken, nil
}

func encodeListSessionsPageToken(token listSessionsPageToken) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", status.Errorf(codes.Internal, "marshal page token: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeListSessionsPageToken(raw string) (listSessionsPageToken, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return listSessionsPageToken{}, status.Error(codes.InvalidArgument, "page_token is invalid")
	}
	var token listSessionsPageToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return listSessionsPageToken{}, status.Error(codes.InvalidArgument, "page_token is invalid")
	}
	return token, nil
}
