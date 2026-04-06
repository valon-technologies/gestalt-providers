package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type Provider struct {
	runner queryRunner
}

var _ gestalt.Provider = (*Provider)(nil)

func NewProvider() *Provider {
	return &Provider{runner: sdkQueryRunner{}}
}

func (p *Provider) Configure(context.Context, string, map[string]any) error {
	return nil
}

func (p *Provider) Execute(ctx context.Context, operation string, params map[string]any, token string) (*gestalt.OperationResult, error) {
	if operation != queryOperationName {
		return nil, fmt.Errorf("unknown operation %q", operation)
	}

	projectID, _ := params[queryParamProjectID].(string)
	if projectID == "" {
		return nil, fmt.Errorf("%s is required", queryParamProjectID)
	}

	sql, _ := params[queryParamSQL].(string)
	if sql == "" {
		return nil, fmt.Errorf("%s is required", queryParamSQL)
	}

	maxResults := intParam(params, queryParamMaxResults, defaultQueryMaxResults)
	if maxResults < 0 {
		maxResults = 0
	}

	iter, err := p.runner.Run(ctx, projectID, token, sql, queryOptions{
		Timeout:      timeDurationMs(intParam(params, queryParamTimeoutMs, defaultQueryTimeoutMs)),
		UseLegacySQL: boolParam(params, queryParamUseLegacySQL, defaultQueryUseLegacySQL),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	rows, err := readRows(iter, maxResults)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(queryResult{
		Schema:      convertSchema(iter.Schema()),
		Rows:        rows,
		TotalRows:   iter.TotalRows(),
		JobComplete: true,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling result: %w", err)
	}

	return &gestalt.OperationResult{
		Status: http.StatusOK,
		Body:   string(body),
	}, nil
}
