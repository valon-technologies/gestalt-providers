package bigquery

import (
	"context"
	"fmt"
	"net/http"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type Provider struct {
	runner queryRunner
}

type QueryInput struct {
	ProjectID    string `json:"project_id" doc:"GCP project ID" required:"true"`
	Query        string `json:"query" doc:"SQL query to execute" required:"true"`
	MaxResults   *int   `json:"max_results,omitempty" doc:"Maximum number of rows to return" default:"500"`
	TimeoutMs    *int   `json:"timeout_ms,omitempty" doc:"Query timeout in milliseconds" default:"60000"`
	UseLegacySQL bool   `json:"use_legacy_sql,omitempty" doc:"Use legacy SQL syntax" default:"false"`
}

var Router = gestalt.MustRouter(
	"bigquery",
	gestalt.Register(
		gestalt.Operation[QueryInput, queryResult]{
			ID:          queryOperationName,
			Method:      http.MethodPost,
			Description: "Execute a BigQuery SQL query",
		},
		(*Provider).query,
	),
)

var _ gestalt.Provider = (*Provider)(nil)

func New() *Provider {
	return &Provider{runner: sdkQueryRunner{}}
}

func (p *Provider) Configure(context.Context, string, map[string]any) error {
	return nil
}

func (p *Provider) query(ctx context.Context, input QueryInput, req gestalt.Request) (gestalt.Response[queryResult], error) {
	if input.ProjectID == "" {
		return gestalt.Response[queryResult]{}, fmt.Errorf("%s is required", queryParamProjectID)
	}
	if input.Query == "" {
		return gestalt.Response[queryResult]{}, fmt.Errorf("%s is required", queryParamSQL)
	}

	maxResults := defaultQueryMaxResults
	if input.MaxResults != nil {
		maxResults = *input.MaxResults
	}
	if maxResults < 0 {
		maxResults = 0
	}

	timeoutMs := defaultQueryTimeoutMs
	if input.TimeoutMs != nil {
		timeoutMs = *input.TimeoutMs
	}

	iter, err := p.runner.Run(ctx, input.ProjectID, req.Token, input.Query, queryOptions{
		Timeout:      timeDurationMs(timeoutMs),
		UseLegacySQL: input.UseLegacySQL,
	})
	if err != nil {
		return gestalt.Response[queryResult]{}, err
	}
	defer func() { _ = iter.Close() }()

	rows, err := readRows(iter, maxResults)
	if err != nil {
		return gestalt.Response[queryResult]{}, err
	}

	return gestalt.OK(queryResult{
		Schema:      convertSchema(iter.Schema()),
		Rows:        rows,
		TotalRows:   iter.TotalRows(),
		JobComplete: true,
	}), nil
}
