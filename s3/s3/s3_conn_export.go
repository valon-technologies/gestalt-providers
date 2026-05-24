package s3

// S3Conn is the object-store client surface used by provider integration tests.
type S3Conn = s3Conn

// ConnectS3ForTest returns a client backed directly by the configured provider.
func ConnectS3ForTest(provider *Provider) (S3Conn, error) {
	return connectS3(provider)
}
