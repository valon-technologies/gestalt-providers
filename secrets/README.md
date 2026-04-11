# Secrets Providers

Secrets providers for [Gestalt](https://github.com/valon-technologies/gestalt).
Each provider implements a secrets engine that `gestaltd` uses to resolve secret
values at runtime from an external vault or key management service.

See the [configuration guide](https://gestaltd.ai/configuration) for setting up
secrets engines and the
[provider development guide](https://gestaltd.ai/providers) for writing custom
secrets providers.

## Available Providers

| Provider | Version | Description |
|----------|---------|-------------|
| [AWS Secrets Manager](aws/) | `0.0.1-alpha.7` | Resolves secrets from AWS Secrets Manager |
| [Azure Key Vault](azure/) | `0.0.1-alpha.7` | Resolves secrets from Azure Key Vault |
| [Google Secret Manager](google/) | `0.0.1-alpha.15` | Resolves secrets from Google Cloud Secret Manager |
| [HashiCorp Vault](vault/) | `0.0.1-alpha.7` | Resolves secrets from HashiCorp Vault KV v2 |
