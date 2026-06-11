# S3 Providers

S3-compatible object-store providers for
[Gestalt](https://github.com/valon-technologies/gestalt).

Each package under `s3/` implements the portable `gestalt.S3Provider`
interface from the core Gestalt SDK.

Current packages:

- `gcs`: Native Google Cloud Storage provider that maps `ObjectRef.versionId`
  to GCS generations and supports GCS generation preconditions.
