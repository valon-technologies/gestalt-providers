# S3 Providers

S3-compatible object-store providers for
[Gestalt](https://github.com/valon-technologies/gestalt).

Each package under `s3/` implements the portable `gestalt.S3Provider`
interface from the core Gestalt SDK and is intended for object stores that
expose an Amazon S3-compatible API.

Current packages:

- `s3`: S3 provider for AWS S3, GCS XML interoperability, MinIO, and similar
  backends.
