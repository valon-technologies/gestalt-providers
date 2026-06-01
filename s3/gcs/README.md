# Google Cloud Storage S3 Provider

Google Cloud Storage provider for Gestalt's `S3Provider` surface. It uses the
native Google Cloud Storage Go client and maps the portable S3 object API onto a
single configured bucket.

## YAML configuration

```yaml
providers:
  s3:
    archive:
      source:
        ref: github.com/valon-technologies/gestalt-providers/s3/gcs
        version: ...
      config:
        bucket: archive-assets
        keyPrefix: app/archive
        userProject: billing-project
```

Supported config fields:

- `bucket`: backing GCS bucket for all object operations.
- `keyPrefix`: optional backend key prefix within the configured bucket. SDK object keys are relative to this prefix.
- `userProject`: optional billing project for Requester Pays buckets.

Credentials are loaded through Google Application Default Credentials.

## Generation preconditions

The provider keeps the public Gestalt S3 contract unchanged:

- `ObjectRef.versionId` / Go `ObjectRef.VersionID` maps to the GCS object generation.
- Returned object metadata sets `Ref.VersionID` to the decimal GCS generation.
- `WriteRequest.IfNoneMatch == "*"` maps to GCS `DoesNotExist`, which is the native create-if-absent precondition.
- Writing or copying to a destination `ObjectRef` with `VersionID` set uses GCS `GenerationMatch`.

GCS metageneration preconditions are not exposed by the current Gestalt S3
surface. ETag-style mutation preconditions other than `IfNoneMatch == "*"` are
rejected instead of being implemented as race-prone best-effort checks.

## Notes

- Reads, heads, deletes, and copy sources can target a specific generation via
  `objectVersion(key, generation)`.
- Signed URLs use GCS V4 signing. The runtime credentials must be able to sign
  bytes, either through service account key material or IAM signing support.
- `PresignObject` includes a `generation` query parameter for versioned GET,
  HEAD, and DELETE URLs. Versioned PUT URLs require the
  `x-goog-if-generation-match` header returned by the provider.
