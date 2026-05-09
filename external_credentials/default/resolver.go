package externalcredentials

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const tokenRefreshThreshold = 5 * time.Minute

var errAmbiguousCredential = errors.New("ambiguous external credential")

type tokenResponse struct {
	AccessToken   string
	RefreshToken  string
	RefreshSource string
	ExpiresIn     int
	TokenType     string
	Extra         map[string]any
}

type tokenEndpointError struct {
	statusCode int
	oauthCode  string
}

func (e *tokenEndpointError) Error() string {
	if e == nil {
		return ""
	}
	if e.oauthCode != "" {
		return fmt.Sprintf("token endpoint returned %d: %s", e.statusCode, e.oauthCode)
	}
	return fmt.Sprintf("token endpoint returned %d", e.statusCode)
}

type resolveState struct {
	group singleflight.Group
}

var resolverState resolveState

func (p *Provider) ValidateCredentialConfig(_ context.Context, req *gestalt.ValidateExternalCredentialConfigRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	return validateCredentialAuthConfig(req.GetMode(), req.GetAuth())
}

func validateCredentialAuthConfig(mode string, auth *gestalt.ExternalCredentialAuthConfig) error {
	if strings.TrimSpace(mode) == "platform" {
		return status.Error(codes.InvalidArgument, "credential mode platform is not supported")
	}
	if auth == nil {
		return nil
	}
	if len(auth.GetTokenExchangeDrivers()) > 1 {
		return status.Error(codes.InvalidArgument, "only one tokenExchangeDriver is supported")
	}
	for _, driver := range auth.GetTokenExchangeDrivers() {
		if driver == nil {
			continue
		}
		switch strings.TrimSpace(driver.GetType()) {
		case "":
			return status.Error(codes.InvalidArgument, "tokenExchangeDriver.type is required")
		case "google_service_account_impersonation":
			if strings.TrimSpace(driver.GetTargetPrincipal()) == "" {
				return status.Error(codes.InvalidArgument, "google_service_account_impersonation targetPrincipal is required")
			}
		default:
			return status.Errorf(codes.InvalidArgument, "unsupported tokenExchangeDriver type %q", driver.GetType())
		}
	}
	switch auth.GetTokenExchange() {
	case "", "form", "json":
	default:
		return status.Errorf(codes.InvalidArgument, "unknown tokenExchange %q", auth.GetTokenExchange())
	}
	if len(auth.GetTokenExchangeDrivers()) > 0 {
		return nil
	}
	return nil
}

func (p *Provider) ResolveCredential(ctx context.Context, req *gestalt.ResolveExternalCredentialRequest) (*gestalt.ResolveExternalCredentialResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := p.ValidateCredentialConfig(ctx, &gestalt.ValidateExternalCredentialConfigRequest{
		Provider:         req.GetProvider(),
		Connection:       req.GetConnection(),
		ConnectionID:     req.GetConnectionId(),
		Mode:             req.GetMode(),
		Auth:             req.GetAuth(),
		ConnectionParams: req.GetConnectionParams(),
	}); err != nil {
		return nil, err
	}
	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	credential, err := resolveStoredCredential(ctx, st, req)
	if err != nil {
		return nil, credentialLookupError(err)
	}
	if shouldRefreshCredential(credential, req.GetAuth(), p.now()) {
		credential, err = p.refreshStoredCredentialOnce(ctx, st, req, credential)
		if err != nil {
			return nil, err
		}
	}
	params := metadataParams(credential.GetMetadataJson())
	for k, v := range req.GetConnectionParams() {
		if _, ok := params[k]; !ok {
			params[k] = v
		}
	}
	return &gestalt.ResolveExternalCredentialResponse{
		Token:        credential.GetAccessToken(),
		ExpiresAt:    credential.GetExpiresAt(),
		MetadataJSON: credential.GetMetadataJson(),
		Params:       params,
		Credential:   credential,
	}, nil
}

func (p *Provider) ExchangeCredential(ctx context.Context, req *gestalt.ExchangeExternalCredentialRequest) (*gestalt.ExchangeExternalCredentialResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := p.ValidateCredentialConfig(ctx, &gestalt.ValidateExternalCredentialConfigRequest{
		Provider:         req.GetProvider(),
		Connection:       req.GetConnection(),
		ConnectionID:     req.GetConnectionId(),
		Mode:             "user",
		Auth:             req.GetAuth(),
		ConnectionParams: req.GetConnectionParams(),
	}); err != nil {
		return nil, err
	}
	auth := req.GetAuth()
	if auth == nil || strings.TrimSpace(auth.GetTokenUrl()) == "" {
		return &gestalt.ExchangeExternalCredentialResponse{}, nil
	}
	resp, err := exchangeManualCredentials(ctx, auth, req.GetCredentialJson(), req.GetConnectionParams())
	if err != nil {
		return nil, status.Error(codes.Unavailable, sanitizeTokenError(err))
	}
	resp.RefreshSource = req.GetCredentialJson()
	return &gestalt.ExchangeExternalCredentialResponse{TokenResponse: tokenResponseToProto(resp)}, nil
}

func resolveStoredCredential(ctx context.Context, st *store, req *gestalt.ResolveExternalCredentialRequest) (*gestalt.ExternalCredential, error) {
	subjectID := strings.TrimSpace(req.GetCredentialSubjectId())
	connectionID := strings.TrimSpace(req.GetConnectionId())
	instance := strings.TrimSpace(req.GetInstance())
	if subjectID == "" {
		return nil, status.Error(codes.InvalidArgument, "credential_subject_id is required")
	}
	if connectionID == "" {
		return nil, status.Error(codes.InvalidArgument, "connection_id is required")
	}
	if instance != "" {
		return st.getCredential(ctx, subjectID, connectionID, instance)
	}
	credentials, err := st.listCredentials(ctx, subjectID, connectionID, "")
	if err != nil {
		return nil, err
	}
	switch len(credentials) {
	case 0:
		return nil, gestalt.ErrExternalCredentialNotFound
	case 1:
		return credentials[0], nil
	default:
		return nil, errAmbiguousCredential
	}
}

func (p *Provider) refreshStoredCredentialOnce(ctx context.Context, st *store, req *gestalt.ResolveExternalCredentialRequest, credential *gestalt.ExternalCredential) (*gestalt.ExternalCredential, error) {
	key := credentialRefreshKey(credential)
	v, err, _ := resolverState.group.Do(key, func() (any, error) {
		return p.refreshStoredCredential(ctx, st, req, credential)
	})
	if err != nil {
		return nil, err
	}
	refreshed, ok := v.(*gestalt.ExternalCredential)
	if !ok || refreshed == nil {
		return nil, status.Error(codes.Internal, "refresh returned no credential")
	}
	return refreshed, nil
}

func credentialRefreshKey(credential *gestalt.ExternalCredential) string {
	if credential == nil {
		return ""
	}
	return credential.GetSubjectId() + ":" + credential.GetConnectionId() + ":" + credential.GetInstance()
}

func (p *Provider) refreshStoredCredential(ctx context.Context, st *store, req *gestalt.ResolveExternalCredentialRequest, credential *gestalt.ExternalCredential) (*gestalt.ExternalCredential, error) {
	resp, err := refreshCredential(ctx, req.GetAuth(), credential.GetRefreshToken(), mergeParams(metadataParams(credential.GetMetadataJson()), req.GetConnectionParams()))
	now := p.now().UTC()
	if err != nil {
		credential.RefreshErrorCount++
		credential.UpdatedAt = utcTimePtr(&now)
		if isTerminalRefreshError(err) {
			expiredAt := now.Add(-1 * time.Hour)
			credential.ExpiresAt = utcTimePtr(&expiredAt)
			marked, markErr := st.upsertCredential(ctx, credential, false, now)
			if markErr != nil {
				return nil, status.Error(codes.Unauthenticated, "token expired or was revoked; reconnect it")
			}
			if deleteErr := st.deleteCredential(ctx, marked.GetId()); deleteErr != nil {
				return nil, status.Error(codes.Unauthenticated, "token expired or was revoked; reconnect it")
			}
			return nil, status.Error(codes.Unauthenticated, "token expired or was revoked; reconnect it")
		}
		_, _ = st.upsertCredential(ctx, credential, false, now)
		if credential.GetExpiresAt() != nil && now.Before(*credential.GetExpiresAt()) {
			return credential, nil
		}
		return nil, status.Error(codes.Unauthenticated, "token expired and refresh failed")
	}
	credential.AccessToken = resp.AccessToken
	if resp.RefreshSource != "" {
		credential.RefreshToken = resp.RefreshSource
	} else if resp.RefreshToken != "" {
		credential.RefreshToken = resp.RefreshToken
	}
	credential.ExpiresAt = utcTimePtr(expiresAtFromExpiresIn(now, resp.ExpiresIn))
	credential.LastRefreshedAt = utcTimePtr(&now)
	credential.RefreshErrorCount = 0
	credential.UpdatedAt = utcTimePtr(&now)
	return st.upsertCredential(ctx, credential, false, now)
}

func shouldRefreshCredential(credential *gestalt.ExternalCredential, auth *gestalt.ExternalCredentialAuthConfig, now time.Time) bool {
	return shouldRefreshCredentialWithin(credential, auth, now, tokenRefreshThreshold)
}

func shouldRefreshCredentialWithin(credential *gestalt.ExternalCredential, auth *gestalt.ExternalCredentialAuthConfig, now time.Time, threshold time.Duration) bool {
	if credential == nil || credential.GetRefreshToken() == "" || credential.GetExpiresAt() == nil {
		return false
	}
	if auth == nil || strings.TrimSpace(auth.GetTokenUrl()) == "" {
		return false
	}
	if threshold <= 0 {
		return false
	}
	return credential.GetExpiresAt().Sub(now) <= threshold
}

func refreshCredential(ctx context.Context, auth *gestalt.ExternalCredentialAuthConfig, refreshToken string, params map[string]string) (*tokenResponse, error) {
	if auth == nil || strings.TrimSpace(auth.GetTokenUrl()) == "" {
		return nil, fmt.Errorf("no token refresher configured")
	}
	switch auth.GetType() {
	case "manual":
		resp, err := exchangeManualCredentials(ctx, auth, refreshToken, params)
		if resp != nil {
			resp.RefreshSource = refreshToken
		}
		return resp, err
	case "oauth2", "":
		return refreshOAuthToken(ctx, auth, refreshToken, params)
	default:
		return nil, fmt.Errorf("unsupported auth type %q", auth.GetType())
	}
}

func exchangeManualCredentials(ctx context.Context, auth *gestalt.ExternalCredentialAuthConfig, credentialJSON string, params map[string]string) (*tokenResponse, error) {
	var credentials map[string]string
	if err := json.Unmarshal([]byte(credentialJSON), &credentials); err != nil {
		return nil, fmt.Errorf("decoding manual credentials: %w", err)
	}
	if len(credentials) == 0 {
		return nil, fmt.Errorf("manual credentials are required")
	}
	values := url.Values{}
	for k, v := range auth.GetTokenParams() {
		values.Set(k, v)
	}
	for k, v := range credentials {
		values.Set(k, v)
	}
	return tokenRequest(ctx, auth, values, interpolate(auth.GetTokenUrl(), params))
}

func refreshOAuthToken(ctx context.Context, auth *gestalt.ExternalCredentialAuthConfig, refreshToken string, params map[string]string) (*tokenResponse, error) {
	values := url.Values{}
	values.Set("grant_type", "refresh_token")
	values.Set("refresh_token", refreshToken)
	if auth.GetClientAuth() != "header" {
		values.Set("client_id", auth.GetClientId())
		values.Set("client_secret", auth.GetClientSecret())
	}
	for k, v := range auth.GetRefreshParams() {
		values.Set(k, v)
	}
	return tokenRequest(ctx, auth, values, interpolate(auth.GetTokenUrl(), params))
}

func tokenRequest(ctx context.Context, auth *gestalt.ExternalCredentialAuthConfig, values url.Values, tokenURL string) (*tokenResponse, error) {
	var body io.Reader
	contentType := "application/x-www-form-urlencoded"
	if auth.GetTokenExchange() == "json" {
		payload := map[string]string{}
		for k, vs := range values {
			if len(vs) > 0 {
				payload[k] = vs[0]
			}
		}
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
		contentType = "application/json"
	} else {
		body = strings.NewReader(values.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	if auth.GetAcceptHeader() != "" {
		req.Header.Set("Accept", auth.GetAcceptHeader())
	}
	if auth.GetClientAuth() == "header" {
		req.SetBasicAuth(url.QueryEscape(auth.GetClientId()), url.QueryEscape(auth.GetClientSecret()))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, newTokenEndpointError(resp.StatusCode, respBody)
	}
	return parseTokenResponse(respBody, auth.GetAccessTokenPath())
}

func newTokenEndpointError(statusCode int, body []byte) error {
	var raw struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(body, &raw)
	return &tokenEndpointError{
		statusCode: statusCode,
		oauthCode:  sanitizeOAuthErrorCode(raw.Error),
	}
}

func sanitizeOAuthErrorCode(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 80 {
		return ""
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '.':
		default:
			return ""
		}
	}
	return value
}

func isTerminalRefreshError(err error) bool {
	var tokenErr *tokenEndpointError
	if !errors.As(err, &tokenErr) {
		return false
	}
	return tokenErr.oauthCode == "invalid_grant"
}

func parseTokenResponse(body []byte, accessTokenPath string) (*tokenResponse, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding token response: %w", err)
	}
	accessToken := ""
	if accessTokenPath != "" {
		if v, ok := extractPath(raw, accessTokenPath).(string); ok {
			accessToken = v
		}
	} else if v, ok := raw["access_token"].(string); ok {
		accessToken = v
	}
	if accessToken == "" {
		return nil, fmt.Errorf("token response missing access_token")
	}
	refreshToken, _ := raw["refresh_token"].(string)
	tokenType, _ := raw["token_type"].(string)
	expiresIn := 0
	switch v := raw["expires_in"].(type) {
	case float64:
		expiresIn = int(v)
	case string:
		expiresIn, _ = strconv.Atoi(v)
	}
	return &tokenResponse{AccessToken: accessToken, RefreshToken: refreshToken, ExpiresIn: expiresIn, TokenType: tokenType, Extra: raw}, nil
}

func tokenResponseToProto(resp *tokenResponse) *gestalt.ExternalCredentialTokenResponse {
	if resp == nil {
		return nil
	}
	extraJSON := ""
	if resp.Extra != nil {
		if data, err := json.Marshal(resp.Extra); err == nil {
			extraJSON = string(data)
		}
	}
	return &gestalt.ExternalCredentialTokenResponse{
		AccessToken:   resp.AccessToken,
		RefreshToken:  resp.RefreshToken,
		RefreshSource: resp.RefreshSource,
		ExpiresIn:     int32(resp.ExpiresIn),
		TokenType:     resp.TokenType,
		ExtraJSON:     extraJSON,
	}
}

func expiresAtFromExpiresIn(now time.Time, expiresIn int) *time.Time {
	if expiresIn <= 0 {
		return nil
	}
	expiresAt := now.Add(time.Duration(expiresIn) * time.Second)
	return &expiresAt
}

func metadataParams(metadataJSON string) map[string]string {
	out := map[string]string{}
	if metadataJSON == "" {
		return out
	}
	_ = json.Unmarshal([]byte(metadataJSON), &out)
	return out
}

func mergeParams(base, override map[string]string) map[string]string {
	out := cloneStringMap(base)
	if out == nil {
		out = map[string]string{}
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func interpolate(raw string, params map[string]string) string {
	out := raw
	for k, v := range params {
		out = strings.ReplaceAll(out, "{"+k+"}", v)
	}
	return out
}

func extractPath(raw map[string]any, path string) any {
	path = strings.TrimPrefix(strings.TrimSpace(path), "$.")
	path = strings.TrimPrefix(path, ".")
	var current any = raw
	for _, part := range strings.Split(path, ".") {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = m[part]
	}
	return current
}

func credentialLookupError(err error) error {
	switch {
	case errors.Is(err, gestalt.ErrExternalCredentialNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, errAmbiguousCredential):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return err
	}
}

func sanitizeTokenError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
