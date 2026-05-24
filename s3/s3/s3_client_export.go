package s3

import (
	s3fake "github.com/valon-technologies/gestalt-providers/s3/s3/internal/fake"
)

// S3Client is the object-store client surface used by provider integration tests.
type S3Client = s3Client

// ConnectS3ForTest returns a client backed directly by the configured provider.
func ConnectS3ForTest(provider *Provider) (S3Client, error) {
	return providerS3ClientAdapter{ProviderS3Client: s3fake.NewProviderS3Client(provider)}, nil
}

type providerS3ClientAdapter struct {
	s3fake.ProviderS3Client
}

func (c providerS3ClientAdapter) Object(bucket, key string) s3Object {
	return s3ObjectAdapter{ProviderObject: c.ProviderS3Client.Object(bucket, key)}
}

type s3ObjectAdapter struct {
	s3fake.ProviderObject
}
