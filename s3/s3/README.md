# S3 Provider

Portable S3 provider for Gestalt. This package speaks the AWS S3-compatible
object API, so the same provider can target Amazon S3, Google Cloud Storage's
XML interoperability endpoint, MinIO, and similar backends.

## YAML configuration

Reference this provider in your Gestalt configuration:

```yaml
providers:
  s3:
    assets:
      source:
        ref: github.com/valon-technologies/gestalt-providers/s3/s3
        version: ...
      config:
        region: us-east-1
        endpoint: https://s3.us-east-1.amazonaws.com
        forcePathStyle: false
        payloadSigning: auto
        accessKeyId: ${AWS_ACCESS_KEY_ID}
        secretAccessKey: secret://aws-secret-access-key

  plugins:
    media:
      source:
        path: ./app/media/manifest.yaml
      s3:
        - assets
```

GCS XML interoperability uses the same provider with a different endpoint and
HMAC credentials:

```yaml
providers:
  s3:
    archive:
      source:
        ref: github.com/valon-technologies/gestalt-providers/s3/s3
        version: ...
      config:
        region: auto
        endpoint: https://storage.googleapis.com
        forcePathStyle: true
        payloadSigning: signed
        accessKeyId: ${GCS_HMAC_ACCESS_KEY}
        secretAccessKey: secret://gcs-hmac-secret
```

Supported config fields:

- `region`: signing region for the backend.
- `endpoint`: optional base endpoint for S3-compatible services.
- `forcePathStyle`: switch between path-style and virtual-host-style requests.
- `payloadSigning`: `auto` uses the AWS SDK default payload-signing behavior; `signed` sends a SHA256 payload hash for direct provider requests to S3-compatible services that reject unsigned payloads.
- `accessKeyId`: optional static HMAC access key.
- `secretAccessKey`: optional static HMAC secret key.
- `sessionToken`: optional session token for temporary credentials.

If static credentials are omitted, the provider uses the AWS SDK default
credential chain.

## Go interface

This provider implements the core repo's `gestalt.S3Provider` interface:

```go
type S3Provider interface {
	gestalt.Provider
	proto.S3Server
}
```

The concrete provider package is constructed with `s3provider.New()`:

```go
package main

import (
	"context"
	"log"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	s3provider "github.com/valon-technologies/gestalt-providers/s3/s3"
)

func main() {
	if err := gestalt.ServeS3Provider(context.Background(), s3provider.New()); err != nil {
		log.Fatal(err)
	}
}
```

Plugins consume the provider through the S3 SDK surface:

```go
ctx := context.Background()

store, err := gestalt.S3()
if err != nil {
	return err
}
defer store.Close()

obj := store.Object("uploads", "avatars/user-123.png")
_, err = obj.WriteBytes(ctx, pngBytes, &gestalt.WriteOptions{
	ContentType: "image/png",
})
if err != nil {
	return err
}
```

## Notes

- Object identity is `{bucket, key, versionID}`. Buckets are chosen by the caller, not fixed in provider config.
- Reads stay streaming across the Gestalt gRPC boundary and the S3 HTTP boundary. Writes stream over gRPC, then stage to a temporary file before `PutObject`.
- `payloadSigning: signed` applies to direct provider requests. Presigned PUT URLs keep unsigned-payload signing so clients can upload arbitrary bodies.
- The provider maps portable errors to gRPC status codes so SDK callers consistently get `ErrS3NotFound`, `ErrS3PreconditionFailed`, and `ErrS3InvalidRange`.
