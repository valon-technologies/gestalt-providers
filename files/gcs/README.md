# Google Cloud Storage FileAPI Provider

Google Cloud Storage-backed FileAPI provider.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
providers:
  fileapis:
    gcs:
      source: github.com/valon-technologies/gestalt-providers/files/gcs
      version: ...
      config:
        bucket: my-fileapi-bucket
        prefix: uploads/
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider implements the Gestalt FileAPI surface on top of Google Cloud
Storage using Application Default Credentials (ADC) or any other ambient GCP
credentials available to the process.

- `bucket` is required.
- `prefix` is optional and is prepended to provider-managed object IDs.
- Blob and File metadata is persisted in GCS object metadata.
- Blob/File IDs are stable provider-defined object names under the configured
  prefix.
- `slice` materializes the sliced bytes into a new blob object.
- Object URLs are process-local, provider-managed, revocable tokens. They are
  not signed public GCS URLs and do not survive provider restarts.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
