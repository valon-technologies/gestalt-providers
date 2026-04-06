package gmail

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const (
	gmailBaseURL = "https://gmail.googleapis.com/gmail/v1/users/me"
)

type Provider struct {
	httpClient *http.Client
}

var _ gestalt.Provider = (*Provider)(nil)

func NewProvider() *Provider {
	return &Provider{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (p *Provider) Configure(context.Context, string, map[string]any) error {
	return nil
}

func (p *Provider) Execute(ctx context.Context, operation string, params map[string]any, token string) (*gestalt.OperationResult, error) {
	if token == "" {
		return nil, fmt.Errorf("token is required")
	}

	switch operation {
	case "messages.send":
		return p.sendMessage(ctx, params, token)
	case "messages.createDraft":
		return p.createDraft(ctx, params, token)
	case "messages.reply":
		return p.replyToMessage(ctx, params, token)
	case "messages.forward":
		return p.forwardMessage(ctx, params, token)
	default:
		return nil, fmt.Errorf("unknown operation: %s", operation)
	}
}

func (p *Provider) doGet(ctx context.Context, url string, token string) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return p.doRequest(req, token)
}

func (p *Provider) doPost(ctx context.Context, url string, body any, token string) (json.RawMessage, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return p.doRequest(req, token)
}

func (p *Provider) doRequest(req *http.Request, token string) (json.RawMessage, error) {
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("gmail API error (status %d): %s", resp.StatusCode, body)
	}

	return body, nil
}

func jsonResult(data any) (*gestalt.OperationResult, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshaling result: %w", err)
	}
	return &gestalt.OperationResult{Status: http.StatusOK, Body: string(body)}, nil
}

func stringParam(params map[string]any, key string) string {
	s, _ := params[key].(string)
	return s
}

func boolParamOr(params map[string]any, key string, defaultVal bool) bool {
	v, ok := params[key].(bool)
	if !ok {
		return defaultVal
	}
	return v
}
