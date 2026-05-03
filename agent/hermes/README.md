# Hermes ACP Agent Provider

`agent/hermes` runs an installed Hermes CLI through the Agent Client Protocol
(ACP). It is intended for Hermes profiles that are already configured, including
custom Vertex AI endpoints backed by Google ADC bearer tokens.

## Configuration

```yaml
providers:
  agent:
    hermes:
      source: github.com/valon-technologies/gestalt-providers/agent/hermes
      config:
        hermesHome: /Users/hugh/.hermes
        hermesCommand: hermes
        hermesArgs: ["acp", "--accept-hooks"]
        workingDirectory: /Users/hugh/src/gestalt-providers
        defaultModel: kimi-k2.6
        modelSwitchingEnabled: false
        accessTokenCommand:
          - gcloud
          - auth
          - application-default
          - print-access-token
        accessTokenEnvVar: OPENAI_API_KEY
        autoApprovePermissions: true
```

`hermesHome` is required. The provider sets `HERMES_HOME` to this directory for
every Hermes ACP subprocess, and stores the ACP session id in its in-memory
Gestalt session record. Hermes uses `HERMES_HOME/state.db` when the provider
reloads that ACP session after a token refresh.

Before every turn, the provider runs `accessTokenCommand`, trims stdout, sets the
configured `accessTokenEnvVar`, starts a fresh `hermes acp` subprocess, calls
`session/load`, and then sends the prompt. This avoids the one-hour lifetime of
Google ADC access tokens while keeping Hermes conversation state in ACP.

Set `accessTokenCommand: []` to skip refresh and rely on inherited environment
or `extraEnv`.

Set `modelSwitchingEnabled: false` for fixed Hermes profiles, such as a custom
Vertex endpoint configured in `HERMES_HOME/config.yaml`. In that mode Gestalt
still records the requested model, but the provider does not call ACP
`session/set_model`; Hermes uses the model already configured in its home.

## Scope

This first cut intentionally does not expose the Gestalt MCP catalog, native
Gestalt tool search, tool grants, or structured output. Hermes may still emit its
own ACP tool-progress updates; they are surfaced as turn events only.

The provider auto-approves ACP permission requests. Hermes CLI must be installed
separately; `Hermes Agent v0.12.0` or newer is expected.
