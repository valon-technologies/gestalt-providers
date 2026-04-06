package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"regexp"
	"sort"
	"strconv"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const slackAPIBaseURL = "https://slack.com/api"

var (
	slackMessageURLPattern = regexp.MustCompile(`https?://[^/]+\.slack\.com/archives/([A-Z0-9]+)/p(\d{10})(\d+)`)
	userMentionPattern     = regexp.MustCompile(`<@(U[A-Z0-9]+)>`)
)

type Provider struct {
	httpClient *http.Client
	baseURL    string
}

type GetMessageInput struct {
	URL     string `json:"url,omitempty" doc:"Slack message URL"`
	Channel string `json:"channel,omitempty" doc:"Channel ID"`
	TS      string `json:"ts,omitempty" doc:"Message timestamp"`
}

type FindUserMentionsInput struct {
	Channel     string `json:"channel" doc:"Channel ID to scan" required:"true"`
	UserID      string `json:"user_id,omitempty" doc:"Optional user ID to filter mentions to"`
	Limit       *int   `json:"limit,omitempty" doc:"Number of messages to scan" default:"100"`
	Oldest      string `json:"oldest,omitempty" doc:"Only include messages after this Unix timestamp"`
	Latest      string `json:"latest,omitempty" doc:"Only include messages before this Unix timestamp"`
	IncludeBots bool   `json:"include_bots,omitempty" doc:"Include bot messages in the scan"`
}

type GetThreadParticipantsInput struct {
	Channel         string `json:"channel" doc:"Channel ID containing the thread" required:"true"`
	TS              string `json:"ts" doc:"Parent message timestamp" required:"true"`
	IncludeUserInfo bool   `json:"include_user_info,omitempty" doc:"Fetch user profile details for participants"`
	IncludeBots     *bool  `json:"include_bots,omitempty" doc:"Include bot users in the participant list" default:"true"`
}

type mention struct {
	UserID      string `json:"user_id"`
	MessageTS   string `json:"message_ts"`
	MentionedBy string `json:"mentioned_by"`
	Text        string `json:"text"`
	Channel     string `json:"channel"`
}

type threadParticipant struct {
	UserID          string `json:"user_id"`
	MessageCount    int    `json:"message_count"`
	FirstReplyTS    string `json:"first_reply_ts"`
	IsThreadStarter bool   `json:"is_thread_starter"`
	DisplayName     string `json:"display_name,omitempty"`
	RealName        string `json:"real_name,omitempty"`
	IsBot           *bool  `json:"is_bot,omitempty"`
}

type getMessageOutput struct {
	Data struct {
		Message map[string]any `json:"message"`
	} `json:"data"`
}

type findUserMentionsOutput struct {
	Data struct {
		Mentions         []mention `json:"mentions"`
		MentionedUserIDs []string  `json:"mentioned_user_ids"`
		TotalMentions    int       `json:"total_mentions"`
		MessagesScanned  int       `json:"messages_scanned"`
	} `json:"data"`
}

type getThreadParticipantsOutput struct {
	Data struct {
		Participants     []threadParticipant `json:"participants"`
		ParticipantCount int                 `json:"participant_count"`
		TotalReplies     int                 `json:"total_replies"`
	} `json:"data"`
}

var Router = gestalt.MustRouter(
	"slack",
	gestalt.Register(
		gestalt.Operation[GetMessageInput, getMessageOutput]{
			ID:          "conversations.getMessage",
			Method:      http.MethodPost,
			Description: "Fetch a single message by Slack URL or channel and timestamp",
		},
		(*Provider).getMessage,
	),
	gestalt.Register(
		gestalt.Operation[FindUserMentionsInput, findUserMentionsOutput]{
			ID:          "conversations.findUserMentions",
			Method:      http.MethodPost,
			Description: "Find Slack user mentions in channel messages",
		},
		(*Provider).findUserMentions,
	),
	gestalt.Register(
		gestalt.Operation[GetThreadParticipantsInput, getThreadParticipantsOutput]{
			ID:          "conversations.getThreadParticipants",
			Method:      http.MethodPost,
			Description: "Get unique participants in a Slack thread",
		},
		(*Provider).getThreadParticipants,
	),
)

var _ gestalt.Provider = (*Provider)(nil)

func New() *Provider {
	return &Provider{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    slackAPIBaseURL,
	}
}

func (p *Provider) Configure(context.Context, string, map[string]any) error {
	return nil
}

func (p *Provider) getMessage(ctx context.Context, input GetMessageInput, req gestalt.Request) (gestalt.Response[getMessageOutput], error) {
	if req.Token == "" {
		return gestalt.Response[getMessageOutput]{}, fmt.Errorf("token is required")
	}

	channel := input.Channel
	ts := input.TS

	if input.URL != "" {
		messageURL := input.URL
		match := slackMessageURLPattern.FindStringSubmatch(messageURL)
		if match == nil {
			return gestalt.Response[getMessageOutput]{}, fmt.Errorf("invalid Slack message URL: %s", messageURL)
		}
		channel = match[1]
		ts = match[2] + "." + match[3]
	}

	if channel == "" || ts == "" {
		return gestalt.Response[getMessageOutput]{}, fmt.Errorf("either url or both channel and ts are required")
	}

	query := neturl.Values{
		"channel":   []string{channel},
		"oldest":    []string{ts},
		"latest":    []string{ts},
		"inclusive": []string{"true"},
		"limit":     []string{"1"},
	}
	data, err := p.slackGET(ctx, "/conversations.history", query, req.Token)
	if err != nil {
		return gestalt.Response[getMessageOutput]{}, err
	}

	messages := mapSlice(data["messages"])
	if len(messages) == 0 {
		return gestalt.Response[getMessageOutput]{}, fmt.Errorf("no message found at timestamp %s", ts)
	}

	var output getMessageOutput
	output.Data.Message = messages[0]
	return gestalt.OK(output), nil
}

func (p *Provider) findUserMentions(ctx context.Context, input FindUserMentionsInput, req gestalt.Request) (gestalt.Response[findUserMentionsOutput], error) {
	if req.Token == "" {
		return gestalt.Response[findUserMentionsOutput]{}, fmt.Errorf("token is required")
	}
	if input.Channel == "" {
		return gestalt.Response[findUserMentionsOutput]{}, fmt.Errorf("channel is required")
	}

	limit := 100
	if input.Limit != nil {
		limit = *input.Limit
	}
	query := neturl.Values{
		"channel": []string{input.Channel},
		"limit":   []string{strconv.Itoa(limit)},
	}
	if input.Oldest != "" {
		query.Set("oldest", input.Oldest)
	}
	if input.Latest != "" {
		query.Set("latest", input.Latest)
	}

	data, err := p.slackGET(ctx, "/conversations.history", query, req.Token)
	if err != nil {
		return gestalt.Response[findUserMentionsOutput]{}, err
	}

	messages := mapSlice(data["messages"])
	mentions := make([]mention, 0)
	mentionedUserIDs := make(map[string]struct{})

	for _, message := range messages {
		if !input.IncludeBots && stringField(message, "bot_id") != "" {
			continue
		}

		text := stringField(message, "text")
		for _, match := range userMentionPattern.FindAllStringSubmatch(text, -1) {
			userID := match[1]
			if input.UserID != "" && userID != input.UserID {
				continue
			}
			mentionedUserIDs[userID] = struct{}{}
			mentions = append(mentions, mention{
				UserID:      userID,
				MessageTS:   stringField(message, "ts"),
				MentionedBy: stringField(message, "user"),
				Text:        text,
				Channel:     input.Channel,
			})
		}
	}

	userIDs := make([]string, 0, len(mentionedUserIDs))
	for userID := range mentionedUserIDs {
		userIDs = append(userIDs, userID)
	}
	sort.Strings(userIDs)

	var output findUserMentionsOutput
	output.Data.Mentions = mentions
	output.Data.MentionedUserIDs = userIDs
	output.Data.TotalMentions = len(mentions)
	output.Data.MessagesScanned = len(messages)
	return gestalt.OK(output), nil
}

func (p *Provider) getThreadParticipants(ctx context.Context, input GetThreadParticipantsInput, req gestalt.Request) (gestalt.Response[getThreadParticipantsOutput], error) {
	if req.Token == "" {
		return gestalt.Response[getThreadParticipantsOutput]{}, fmt.Errorf("token is required")
	}
	if input.Channel == "" {
		return gestalt.Response[getThreadParticipantsOutput]{}, fmt.Errorf("channel is required")
	}
	if input.TS == "" {
		return gestalt.Response[getThreadParticipantsOutput]{}, fmt.Errorf("ts is required")
	}

	query := neturl.Values{
		"channel": []string{input.Channel},
		"ts":      []string{input.TS},
		"limit":   []string{"1000"},
	}
	data, err := p.slackGET(ctx, "/conversations.replies", query, req.Token)
	if err != nil {
		return gestalt.Response[getThreadParticipantsOutput]{}, err
	}

	messages := mapSlice(data["messages"])
	includeBots := true
	if input.IncludeBots != nil {
		includeBots = *input.IncludeBots
	}
	threadStarter := ""
	if len(messages) > 0 {
		threadStarter = stringField(messages[0], "user")
	}

	participantsByUser := make(map[string]*threadParticipant)
	for _, message := range messages {
		userID := stringField(message, "user")
		if userID == "" {
			continue
		}
		if !includeBots && stringField(message, "bot_id") != "" {
			continue
		}

		participant := participantsByUser[userID]
		if participant == nil {
			participant = &threadParticipant{
				UserID:          userID,
				MessageCount:    0,
				FirstReplyTS:    stringField(message, "ts"),
				IsThreadStarter: userID == threadStarter,
			}
			participantsByUser[userID] = participant
		}
		participant.MessageCount++
	}

	participants := make([]threadParticipant, 0, len(participantsByUser))
	for _, participant := range participantsByUser {
		if input.IncludeUserInfo {
			userData, err := p.slackGET(ctx, "/users.info", neturl.Values{
				"user": []string{participant.UserID},
			}, req.Token)
			if err == nil {
				user := mapField(userData, "user")
				profile := mapField(user, "profile")
				participant.DisplayName = stringField(profile, "display_name")
				participant.RealName = stringField(user, "real_name")
				if isBot, ok := boolField(user, "is_bot"); ok {
					participant.IsBot = &isBot
				}
			}
		}
		participants = append(participants, *participant)
	}

	sort.Slice(participants, func(i, j int) bool {
		if participants[i].FirstReplyTS == participants[j].FirstReplyTS {
			return participants[i].UserID < participants[j].UserID
		}
		return participants[i].FirstReplyTS < participants[j].FirstReplyTS
	})

	totalReplies := 0
	if len(messages) > 0 {
		totalReplies = len(messages) - 1
	}

	var output getThreadParticipantsOutput
	output.Data.Participants = participants
	output.Data.ParticipantCount = len(participants)
	output.Data.TotalReplies = totalReplies
	return gestalt.OK(output), nil
}

func (p *Provider) slackGET(ctx context.Context, endpoint string, query neturl.Values, token string) (map[string]any, error) {
	return p.slackRequest(ctx, http.MethodGet, endpoint, query, nil, token)
}

func (p *Provider) slackRequest(ctx context.Context, method, endpoint string, query neturl.Values, payload any, token string) (map[string]any, error) {
	requestURL := p.baseURL + endpoint
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}

	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal Slack payload: %w", err)
		}
		body = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	var data map[string]any
	if err := json.Unmarshal(responseBody, &data); err != nil {
		return nil, fmt.Errorf("decode Slack response: %w", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("slack API error (status %d): %s", resp.StatusCode, string(responseBody))
	}
	if ok, exists := boolField(data, "ok"); exists && !ok {
		if slackErr := stringField(data, "error"); slackErr != "" {
			return nil, fmt.Errorf("slack API error: %s", slackErr)
		}
		return nil, fmt.Errorf("slack API error")
	}

	return data, nil
}

func mapSlice(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if object, ok := item.(map[string]any); ok {
			out = append(out, object)
		}
	}
	return out
}

func mapField(data map[string]any, key string) map[string]any {
	if value, ok := data[key].(map[string]any); ok {
		return value
	}
	return nil
}

func stringField(data map[string]any, key string) string {
	if data == nil {
		return ""
	}
	value, ok := data[key]
	if !ok {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return ""
	}
}

func boolField(data map[string]any, key string) (bool, bool) {
	if data == nil {
		return false, false
	}
	value, ok := data[key]
	if !ok {
		return false, false
	}
	b, ok := value.(bool)
	return b, ok
}
