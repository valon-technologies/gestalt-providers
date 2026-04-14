# Amazon S3 FileAPI Provider

Stores Gestalt FileAPI blobs and files in Amazon S3.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
providers:
  fileapis:
    s3:
      source: github.com/valon-technologies/gestalt-providers/fileapi/s3
      version: ...
      config:
        bucket: my-gestalt-files
        region: us-east-1
        prefix: app-a
        endpoint: http://localhost:4566
        forcePathStyle: true
```

`bucket` and `region` are required. `prefix`, `endpoint`, and
`forcePathStyle` are optional.

## Overview

Each FileAPI object is stored as an S3 object under `prefix/objects/<uuid>`.
The provider writes enough S3 metadata to reconstruct the Gestalt Blob/File
shape on later reads:

- object kind (`blob` or `file`)
- MIME type
- file name, when present
- file `lastModified`, when present

The provider uses the standard AWS SDK default credential chain.

Object URLs are provider-managed process-local URLs backed by an in-memory
token map. They are revocable, but they do not survive provider restarts.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
