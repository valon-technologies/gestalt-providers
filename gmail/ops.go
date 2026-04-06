package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func (p *Provider) sendMessage(ctx context.Context, input SendMessageInput, req gestalt.Request) (gestalt.Response[messageOutput], error) {
	if req.Token == "" {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("token is required")
	}
	if input.To == "" || input.Subject == "" || input.Body == "" {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("to, subject, and body are required")
	}

	raw := buildMIME(mimeParams{
		To:       input.To,
		Subject:  input.Subject,
		Body:     input.Body,
		Cc:       input.Cc,
		Bcc:      input.Bcc,
		HtmlBody: input.HTMLBody,
	})

	resp, err := p.doPost(ctx, gmailBaseURL+"/messages/send", map[string]string{"raw": raw}, req.Token)
	if err != nil {
		return gestalt.Response[messageOutput]{}, err
	}
	var output messageOutput
	output.Data.Message = resp
	return gestalt.OK(output), nil
}

func (p *Provider) createDraft(ctx context.Context, input CreateDraftInput, req gestalt.Request) (gestalt.Response[draftOutput], error) {
	if req.Token == "" {
		return gestalt.Response[draftOutput]{}, fmt.Errorf("token is required")
	}
	if input.To == "" || input.Subject == "" || input.Body == "" {
		return gestalt.Response[draftOutput]{}, fmt.Errorf("to, subject, and body are required")
	}

	raw := buildMIME(mimeParams{
		To:       input.To,
		Subject:  input.Subject,
		Body:     input.Body,
		Cc:       input.Cc,
		Bcc:      input.Bcc,
		HtmlBody: input.HTMLBody,
	})

	resp, err := p.doPost(ctx, gmailBaseURL+"/drafts", map[string]any{
		"message": map[string]string{"raw": raw},
	}, req.Token)
	if err != nil {
		return gestalt.Response[draftOutput]{}, err
	}
	var output draftOutput
	output.Data.Draft = resp
	return gestalt.OK(output), nil
}

func (p *Provider) replyToMessage(ctx context.Context, input ReplyMessageInput, req gestalt.Request) (gestalt.Response[messageOutput], error) {
	if req.Token == "" {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("token is required")
	}
	if input.MessageID == "" || input.Body == "" {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("message_id and body are required")
	}

	origURL := gmailBaseURL + "/messages/" + url.PathEscape(input.MessageID) + "?format=metadata&metadataHeaders=From&metadataHeaders=To&metadataHeaders=Cc&metadataHeaders=Subject&metadataHeaders=Message-ID&metadataHeaders=References&metadataHeaders=Delivered-To"
	origBody, err := p.doGet(ctx, origURL, req.Token)
	if err != nil {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("fetching original message: %w", err)
	}

	var orig struct {
		ThreadID string `json:"threadId"`
		Payload  struct {
			Headers []map[string]string `json:"headers"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(origBody, &orig); err != nil {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("parsing original message: %w", err)
	}

	origFrom := getHeader(orig.Payload.Headers, "From")
	origTo := getHeader(orig.Payload.Headers, "To")
	origCc := getHeader(orig.Payload.Headers, "Cc")
	origSubject := getHeader(orig.Payload.Headers, "Subject")
	origMessageID := getHeader(orig.Payload.Headers, "Message-ID")
	origReferences := getHeader(orig.Payload.Headers, "References")

	references := origMessageID
	if origReferences != "" {
		references = origReferences + " " + origMessageID
	}

	selfEmail := getHeader(orig.Payload.Headers, "Delivered-To")

	to := origFrom
	cc := input.Cc
	if input.ReplyAll {
		var allCC []string
		for _, v := range []string{origTo, origCc, cc} {
			if v != "" {
				allCC = append(allCC, v)
			}
		}
		cc = filterSelfFromRecipients(strings.Join(allCC, ", "), selfEmail)
	}

	raw := buildMIME(mimeParams{
		To:         to,
		Subject:    ensureReplyPrefix(origSubject),
		Body:       input.Body,
		Cc:         cc,
		HtmlBody:   input.HTMLBody,
		InReplyTo:  origMessageID,
		References: references,
	})

	resp, err := p.doPost(ctx, gmailBaseURL+"/messages/send", map[string]any{
		"raw":      raw,
		"threadId": orig.ThreadID,
	}, req.Token)
	if err != nil {
		return gestalt.Response[messageOutput]{}, err
	}
	var output messageOutput
	output.Data.Message = resp
	return gestalt.OK(output), nil
}

func (p *Provider) forwardMessage(ctx context.Context, input ForwardMessageInput, req gestalt.Request) (gestalt.Response[messageOutput], error) {
	if req.Token == "" {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("token is required")
	}
	if input.MessageID == "" || input.To == "" {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("message_id and to are required")
	}

	origURL := gmailBaseURL + "/messages/" + url.PathEscape(input.MessageID) + "?format=full"
	origBody, err := p.doGet(ctx, origURL, req.Token)
	if err != nil {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("fetching original message: %w", err)
	}

	var orig struct {
		Payload struct {
			Headers []map[string]string `json:"headers"`
			Body    struct {
				Data string `json:"data"`
			} `json:"body"`
			Parts []struct {
				MimeType string `json:"mimeType"`
				Body     struct {
					Data string `json:"data"`
				} `json:"body"`
			} `json:"parts"`
			MimeType string `json:"mimeType"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(origBody, &orig); err != nil {
		return gestalt.Response[messageOutput]{}, fmt.Errorf("parsing original message: %w", err)
	}

	origSubject := getHeader(orig.Payload.Headers, "Subject")
	origFrom := getHeader(orig.Payload.Headers, "From")
	origDate := getHeader(orig.Payload.Headers, "Date")

	origText := extractPlainText(orig.Payload.Parts, orig.Payload.Body.Data, orig.Payload.MimeType)

	forwardedBody := ""
	if input.AdditionalText != "" {
		forwardedBody = input.AdditionalText + "\r\n\r\n"
	}
	forwardedBody += fmt.Sprintf("---------- Forwarded message ----------\r\nFrom: %s\r\nDate: %s\r\nSubject: %s\r\n\r\n%s",
		origFrom, origDate, origSubject, origText)

	raw := buildMIME(mimeParams{
		To:      input.To,
		Subject: ensureForwardPrefix(origSubject),
		Body:    forwardedBody,
		Cc:      input.Cc,
	})

	resp, err := p.doPost(ctx, gmailBaseURL+"/messages/send", map[string]string{"raw": raw}, req.Token)
	if err != nil {
		return gestalt.Response[messageOutput]{}, err
	}
	var output messageOutput
	output.Data.Message = resp
	return gestalt.OK(output), nil
}

func extractPlainText(parts []struct {
	MimeType string `json:"mimeType"`
	Body     struct {
		Data string `json:"data"`
	} `json:"body"`
}, bodyData, mimeType string) string {
	for _, part := range parts {
		if part.MimeType == "text/plain" && part.Body.Data != "" {
			if decoded, err := b64.DecodeString(part.Body.Data); err == nil {
				return string(decoded)
			}
		}
	}
	if bodyData != "" && !strings.Contains(mimeType, "html") {
		if decoded, err := b64.DecodeString(bodyData); err == nil {
			return string(decoded)
		}
	}
	return ""
}
