# Anthropic Model Provider

`model/anthropic` runs stateless model inference through Anthropic's Messages
API. It implements the Gestalt model provider surface for text messages,
usage reporting, and JSON-schema structured output.

## Local Usage

```yaml
providers:
  secrets:
    env:
      source: env

  model:
    anthropic:
      source: /absolute/path/to/gestalt-providers/model/anthropic/manifest.yaml
      default: true
      env:
        ANTHROPIC_API_KEY:
          secret:
            provider: secrets
            name: anthropic-api-key
      config:
        defaultModel: claude-sonnet-4-5-20250929
        maxTokens: 1024
        timeoutSeconds: 300
        maxRetries: 0
        temperature: 0.2
```

`apiKeyEnv` defaults to `ANTHROPIC_API_KEY`. The provider reads that environment
variable when handling a model request. `config.apiKey` is accepted for local
testing, but provider-level `env` secret injection is preferred for production.

Requests with `response_schema` use a single required synthetic Anthropic tool
with `strict: true`. The tool input schema is the supplied JSON schema, and the
provider returns the tool use `input` as `structured_output`. Requests without a
schema concatenate Anthropic text content blocks into `output_text`.

`maxRetries` defaults to `0`, so retry policy stays with the caller or Gestalt
runtime rather than multiplying the Anthropic SDK's retry loop. `timeoutSeconds`
defaults to `300`.
