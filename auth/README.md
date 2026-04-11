# Authentication Providers

Authentication providers for [Gestalt](https://github.com/valon-technologies/gestalt).
Each provider implements an authentication backend that `gestaltd` uses to verify
user identity and manage sessions.

See the [configuration guide](https://gestaltd.ai/configuration) for setting up
auth backends and the
[provider development guide](https://gestaltd.ai/providers) for writing custom
auth providers.

## Available Providers

| Provider | Version | Description |
|----------|---------|-------------|
| [Google](google/) | `0.0.1-alpha.10` | Authenticate users with Google OAuth and validate Google bearer tokens |
| [OIDC](oidc/) | `0.0.1-alpha.9` | Authenticate users with an OpenID Connect provider |
