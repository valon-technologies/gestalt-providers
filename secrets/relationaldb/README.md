# Relational Database Secrets

Resolves deployment secrets from a relational database. Values are encrypted
with Google Cloud KMS and bound to their logical names as additional
authenticated data, so ciphertext cannot be moved between names.

The database credential is intentionally not stored in this provider. Supply it
through the `GESTALT_SECRETS_DSN` environment variable from the deployment's
bootstrap secret manager. `kmsKey` is a non-secret resource name; the provider
uses Application Default Credentials for KMS.

```yaml
providers:
  secrets:
    secrets:
      source:
        ref: github.com/valon-technologies/gestalt-providers/secrets/relationaldb
        version: 0.0.1-alpha.1
      config:
        dsn: ${GESTALT_SECRETS_DSN}
        schema: vt
        kmsKey: projects/example/locations/us-east1/keyRings/gestalt/cryptoKeys/runtime-secrets
```

The provider is runtime-only and never creates or mutates the table.
