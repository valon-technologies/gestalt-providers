package s3

import (
	"context"
	"io"

	s3fake "github.com/valon-technologies/gestalt-providers/s3/s3/internal/fake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func wrapProviderS3Client(provider gestalt.S3Provider) s3Client {
	return s3ClientAdapter{ProviderS3Client: s3fake.NewProviderS3Client(provider)}
}

type s3ClientAdapter struct {
	s3fake.ProviderS3Client
}

func (c s3ClientAdapter) Object(bucket, key string) s3Object {
	return s3ObjectAdapter{ProviderObject: c.ProviderS3Client.Object(bucket, key)}
}

type s3ObjectAdapter struct {
	s3fake.ProviderObject
}

func (o s3ObjectAdapter) Stream(ctx context.Context, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error) {
	return o.ProviderObject.Stream(ctx, opts)
}
