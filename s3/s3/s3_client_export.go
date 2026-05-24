package s3

// S3Client is the object-store client surface used by provider integration tests.
type S3Client = s3Client

// ConnectS3ForTest returns a client backed directly by the configured provider.
func ConnectS3ForTest(provider *Provider) (S3Client, error) {
	return wrapProviderS3Client(provider), nil
}
