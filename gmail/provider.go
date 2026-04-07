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

type SendMessageInput struct {
	To       string `json:"to" doc:"Recipient email address" required:"true"`
	Subject  string `json:"subject" doc:"Email subject" required:"true"`
	Body     string `json:"body" doc:"Plain text body" required:"true"`
	Cc       string `json:"cc,omitempty" doc:"CC recipients (comma-separated)"`
	Bcc      string `json:"bcc,omitempty" doc:"BCC recipients (comma-separated)"`
	HTMLBody string `json:"html_body,omitempty" doc:"HTML body (sent as alternative to plain text)"`
}

type CreateDraftInput struct {
	To       string `json:"to" doc:"Recipient email address" required:"true"`
	Subject  string `json:"subject" doc:"Email subject" required:"true"`
	Body     string `json:"body" doc:"Plain text body" required:"true"`
	Cc       string `json:"cc,omitempty" doc:"CC recipients (comma-separated)"`
	Bcc      string `json:"bcc,omitempty" doc:"BCC recipients (comma-separated)"`
	HTMLBody string `json:"html_body,omitempty" doc:"HTML body"`
}

type ReplyMessageInput struct {
	MessageID string `json:"message_id" doc:"Original message ID" required:"true"`
	Body      string `json:"body" doc:"Reply body" required:"true"`
	Cc        string `json:"cc,omitempty" doc:"CC recipients (comma-separated)"`
	ReplyAll  bool   `json:"reply_all,omitempty" doc:"Reply to all recipients"`
	HTMLBody  string `json:"html_body,omitempty" doc:"HTML body"`
}

type ForwardMessageInput struct {
	MessageID      string `json:"message_id" doc:"Message to forward" required:"true"`
	To             string `json:"to" doc:"Forward recipient" required:"true"`
	AdditionalText string `json:"additional_text,omitempty" doc:"Text to prepend to forwarded content"`
	Cc             string `json:"cc,omitempty" doc:"CC recipients (comma-separated)"`
}

type messageOutput struct {
	Data struct {
		Message json.RawMessage `json:"message"`
	} `json:"data"`
}

type draftOutput struct {
	Data struct {
		Draft json.RawMessage `json:"draft"`
	} `json:"data"`
}

var Router = gestalt.MustNamedRouter(
	"gmail",
	gestalt.Register(
		gestalt.Operation[SendMessageInput, messageOutput]{
			ID:          "messages.send",
			Method:      http.MethodPost,
			Description: "Send an email message",
		},
		(*Provider).sendMessage,
	),
	gestalt.Register(
		gestalt.Operation[CreateDraftInput, draftOutput]{
			ID:          "messages.createDraft",
			Method:      http.MethodPost,
			Description: "Create an email draft",
		},
		(*Provider).createDraft,
	),
	gestalt.Register(
		gestalt.Operation[ReplyMessageInput, messageOutput]{
			ID:          "messages.reply",
			Method:      http.MethodPost,
			Description: "Reply to an existing message",
		},
		(*Provider).replyToMessage,
	),
	gestalt.Register(
		gestalt.Operation[ForwardMessageInput, messageOutput]{
			ID:          "messages.forward",
			Method:      http.MethodPost,
			Description: "Forward a message to new recipients",
		},
		(*Provider).forwardMessage,
	),
)

var _ gestalt.Provider = (*Provider)(nil)

func New() *Provider {
	return &Provider{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (p *Provider) Configure(context.Context, string, map[string]any) error {
	return nil
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
