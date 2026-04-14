# AWS Secrets Manager

Resolves secrets from AWS Secrets Manager.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
secrets:
  aws:
    source: github.com/valon-technologies/gestalt-providers/secrets/aws
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider resolves secret references in your Gestalt configuration against
AWS Secrets Manager. At runtime, `gestaltd` fetches the secret values using
the standard AWS credential chain (environment variables, shared credentials
file, IAM role, etc.).

## Config Interface

```go
type config struct {
    Region            string `yaml:"region"`
    VersionStage      string `yaml:"versionStage"`
    Endpoint          string `yaml:"endpoint"`
    AllowInsecureHTTP bool   `yaml:"allowInsecureHttp"`
}
```

`endpoint` is optional. When it is set, it must use `https://` by default.
Set `allowInsecureHttp: true` only for local loopback development with
`http://localhost`, `http://127.0.0.1`, or `http://[::1]`.

## Examples

Use the default AWS Secrets Manager endpoint:

```yaml
secrets:
  aws:
    source: github.com/valon-technologies/gestalt-providers/secrets/aws
    version: ...
    config:
      region: us-east-1
      versionStage: AWSCURRENT
```

Use a local loopback endpoint for LocalStack development:

```yaml
secrets:
  aws:
    source: github.com/valon-technologies/gestalt-providers/secrets/aws
    version: ...
    config:
      region: us-east-1
      endpoint: http://127.0.0.1:4566
      allowInsecureHttp: true
```

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
