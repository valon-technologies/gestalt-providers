# Default External Credentials

This provider implements the `external_credentials` primitive using the host
IndexedDB service exposed through `gestalt.IndexedDB(...)`.

It persists credentials in the `external_credentials` object store and keeps the
same encrypted token field format used by `gestaltd/core/crypto`: AES-256-GCM
with a nonce-prefixed ciphertext encoded via standard base64. To read rows
written by the in-process provider, configure the same `encryptionKey` value the
server used before.

## Configuration

```yaml
providers:
  indexeddb:
    default:
      source:
        ref: github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb
        version: 0.0.1-alpha.5

  externalCredentials:
    default:
      source:
        ref: github.com/valon-technologies/gestalt-providers/external_credentials/default
        version: 0.0.1-alpha.1
      config:
        encryptionKey: ${GESTALT_ENCRYPTION_KEY}

server:
  providers:
    indexeddb: default
    externalCredentials: default
```

Configuration fields:

- `encryptionKey`: Required. Must match the key material used for existing
  `external_credentials` rows.
- `indexeddb`: Optional named host IndexedDB provider to connect to. Omit it to
  use the default IndexedDB socket.

## Persisted Store

The provider owns the `external_credentials` store directly with these indexes:

- `by_subject`
- `by_subject_integration`
- `by_subject_connection`
- `by_lookup`
