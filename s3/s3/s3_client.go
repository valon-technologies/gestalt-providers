package s3

import (
	"context"
	"io"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type s3Client interface {
	Close() error
	Object(bucket, key string) s3Object
	HeadObject(ctx context.Context, ref gestalt.ObjectRef) (gestalt.ObjectMeta, error)
	ReadObject(ctx context.Context, ref gestalt.ObjectRef, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error)
	WriteObject(ctx context.Context, ref gestalt.ObjectRef, body io.Reader, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error)
	DeleteObject(ctx context.Context, ref gestalt.ObjectRef) error
	ListObjects(ctx context.Context, opts gestalt.ListOptions) (gestalt.ListPage, error)
	CopyObject(ctx context.Context, source, destination gestalt.ObjectRef, opts *gestalt.CopyOptions) (gestalt.ObjectMeta, error)
	PresignObject(ctx context.Context, ref gestalt.ObjectRef, opts *gestalt.PresignOptions) (gestalt.PresignResult, error)
}

type s3Object interface {
	Stat(ctx context.Context) (gestalt.ObjectMeta, error)
	Exists(ctx context.Context) (bool, error)
	Stream(ctx context.Context, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error)
	Bytes(ctx context.Context, opts *gestalt.ReadOptions) ([]byte, error)
	Text(ctx context.Context, opts *gestalt.ReadOptions) (string, error)
	JSON(ctx context.Context, opts *gestalt.ReadOptions) (any, error)
	Write(ctx context.Context, body io.Reader, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error)
	WriteBytes(ctx context.Context, body []byte, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error)
	WriteString(ctx context.Context, body string, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error)
	WriteJSON(ctx context.Context, value any, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error)
	Delete(ctx context.Context) error
	Presign(ctx context.Context, opts *gestalt.PresignOptions) (gestalt.PresignResult, error)
}

var connectS3 = func() (s3Client, error) {
	client, err := gestalt.S3()
	if err != nil {
		return nil, err
	}
	return sdkS3Client{client}, nil
}

type sdkS3Client struct {
	*gestalt.S3Client
}

func (c sdkS3Client) Object(bucket, key string) s3Object {
	return sdkS3Object{client: c.S3Client, ref: gestalt.ObjectRef{Bucket: bucket, Key: key}}
}

type sdkS3Object struct {
	client *gestalt.S3Client
	ref    gestalt.ObjectRef
}

func (o sdkS3Object) Stat(ctx context.Context) (gestalt.ObjectMeta, error) {
	return o.client.HeadObject(ctx, o.ref)
}

func (o sdkS3Object) Exists(ctx context.Context) (bool, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).Exists(ctx)
}

func (o sdkS3Object) Stream(ctx context.Context, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error) {
	return o.client.ReadObject(ctx, o.ref, opts)
}

func (o sdkS3Object) Bytes(ctx context.Context, opts *gestalt.ReadOptions) ([]byte, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).Bytes(ctx, opts)
}

func (o sdkS3Object) Text(ctx context.Context, opts *gestalt.ReadOptions) (string, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).Text(ctx, opts)
}

func (o sdkS3Object) JSON(ctx context.Context, opts *gestalt.ReadOptions) (any, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).JSON(ctx, opts)
}

func (o sdkS3Object) Write(ctx context.Context, body io.Reader, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.client.WriteObject(ctx, o.ref, body, opts)
}

func (o sdkS3Object) WriteBytes(ctx context.Context, body []byte, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).WriteBytes(ctx, body, opts)
}

func (o sdkS3Object) WriteString(ctx context.Context, body string, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).WriteString(ctx, body, opts)
}

func (o sdkS3Object) WriteJSON(ctx context.Context, value any, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).WriteJSON(ctx, value, opts)
}

func (o sdkS3Object) Delete(ctx context.Context) error {
	return o.client.DeleteObject(ctx, o.ref)
}

func (o sdkS3Object) Presign(ctx context.Context, opts *gestalt.PresignOptions) (gestalt.PresignResult, error) {
	return o.client.PresignObject(ctx, o.ref, opts)
}
