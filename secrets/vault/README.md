# HashiCorp Vault

Resolves secrets from HashiCorp Vault KV v2.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
secrets:
  vault:
    source: github.com/valon-technologies/gestalt-providers/secrets/vault
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider resolves secret references in your Gestalt configuration against
a HashiCorp Vault server using the KV v2 secrets engine. At runtime, `gestaltd`
authenticates to Vault and fetches the requested secret paths.

## Config Interface

```go
type config struct {
    Address           string `yaml:"address"`
    Token             string `yaml:"token"`
    MountPath         string `yaml:"mountPath"`
    Namespace         string `yaml:"namespace"`
    AllowInsecureHTTP bool   `yaml:"allowInsecureHttp"`
}
```

`address` must use `https://` by default. Set `allowInsecureHttp: true` only
for local loopback development with `http://localhost`, `http://127.0.0.1`, or
`http://[::1]`.

## Examples

Use a production Vault endpoint over HTTPS:

```yaml
secrets:
  vault:
    source: github.com/valon-technologies/gestalt-providers/secrets/vault
    version: ...
    config:
      address: https://vault.example.com
      token: ${VAULT_TOKEN}
      mountPath: secret
```

Use a local loopback Vault dev server:

```yaml
secrets:
  vault:
    source: github.com/valon-technologies/gestalt-providers/secrets/vault
    version: ...
    config:
      address: http://127.0.0.1:8200
      token: ${VAULT_TOKEN}
      mountPath: secret
      allowInsecureHttp: true
```

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
