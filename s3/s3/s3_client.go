package s3

import (
	"context"
	"fmt"
	"io"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	sdkS3 "github.com/valon-technologies/gestalt/sdk/go/s3"
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
	client, err := gestalt.S3(context.Background())
	if err != nil {
		return nil, err
	}
	hostClient, ok := client.(*sdkS3.HostClient)
	if !ok {
		return nil, fmt.Errorf("s3: unexpected client type %T", client)
	}
	return hostS3Client{hostClient}, nil
}

type hostS3Client struct {
	*sdkS3.HostClient
}

func (c hostS3Client) Object(bucket, key string) s3Object {
	return hostS3Object{client: c.HostClient, ref: gestalt.ObjectRef{Bucket: bucket, Key: key}}
}

func (c hostS3Client) ReadObject(ctx context.Context, ref gestalt.ObjectRef, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error) {
	result, err := c.HostClient.ReadObject(ctx, readRequest(ref, opts))
	if err != nil {
		return gestalt.ObjectMeta{}, nil, err
	}
	return result.Meta, result.Body, nil
}

func (c hostS3Client) WriteObject(ctx context.Context, ref gestalt.ObjectRef, body io.Reader, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return c.HostClient.WriteObject(ctx, writeRequest(ref, body, opts))
}

func (c hostS3Client) ListObjects(ctx context.Context, opts gestalt.ListOptions) (gestalt.ListPage, error) {
	return c.HostClient.ListObjects(ctx, listRequest(opts))
}

func (c hostS3Client) CopyObject(ctx context.Context, source, destination gestalt.ObjectRef, opts *gestalt.CopyOptions) (gestalt.ObjectMeta, error) {
	return c.HostClient.CopyObject(ctx, copyRequest(source, destination, opts))
}

func (c hostS3Client) PresignObject(ctx context.Context, ref gestalt.ObjectRef, opts *gestalt.PresignOptions) (gestalt.PresignResult, error) {
	return c.HostClient.PresignObject(ctx, presignRequest(ref, opts))
}

func readRequest(ref gestalt.ObjectRef, opts *gestalt.ReadOptions) sdkS3.ReadRequest {
	req := sdkS3.ReadRequest{Ref: ref}
	if opts == nil {
		return req
	}
	req.Range = opts.Range
	req.IfMatch = opts.IfMatch
	req.IfNoneMatch = opts.IfNoneMatch
	req.IfModifiedSince = opts.IfModifiedSince
	req.IfUnmodifiedSince = opts.IfUnmodifiedSince
	return req
}

func writeRequest(ref gestalt.ObjectRef, body io.Reader, opts *gestalt.WriteOptions) sdkS3.WriteRequest {
	req := sdkS3.WriteRequest{Ref: ref, Body: body}
	if opts == nil {
		return req
	}
	req.ContentType = opts.ContentType
	req.CacheControl = opts.CacheControl
	req.ContentDisposition = opts.ContentDisposition
	req.ContentEncoding = opts.ContentEncoding
	req.ContentLanguage = opts.ContentLanguage
	req.Metadata = opts.Metadata
	req.IfMatch = opts.IfMatch
	req.IfNoneMatch = opts.IfNoneMatch
	return req
}

func listRequest(opts gestalt.ListOptions) sdkS3.ListRequest {
	return sdkS3.ListRequest{
		Bucket:            opts.Bucket,
		Prefix:            opts.Prefix,
		Delimiter:         opts.Delimiter,
		ContinuationToken: opts.ContinuationToken,
		StartAfter:        opts.StartAfter,
		MaxKeys:           opts.MaxKeys,
	}
}

func copyRequest(source, destination gestalt.ObjectRef, opts *gestalt.CopyOptions) sdkS3.CopyRequest {
	req := sdkS3.CopyRequest{Source: source, Destination: destination}
	if opts == nil {
		return req
	}
	req.IfMatch = opts.IfMatch
	req.IfNoneMatch = opts.IfNoneMatch
	return req
}

func presignRequest(ref gestalt.ObjectRef, opts *gestalt.PresignOptions) sdkS3.PresignRequest {
	req := sdkS3.PresignRequest{Ref: ref}
	if opts == nil {
		return req
	}
	req.Method = opts.Method
	req.Expires = opts.Expires
	req.ContentType = opts.ContentType
	req.ContentDisposition = opts.ContentDisposition
	req.Headers = opts.Headers
	return req
}

type hostS3Object struct {
	client *sdkS3.HostClient
	ref    gestalt.ObjectRef
}

func (o hostS3Object) Stat(ctx context.Context) (gestalt.ObjectMeta, error) {
	return o.client.HeadObject(ctx, o.ref)
}

func (o hostS3Object) Exists(ctx context.Context) (bool, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).Exists(ctx)
}

func (o hostS3Object) Stream(ctx context.Context, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error) {
	result, err := o.client.ReadObject(ctx, readRequest(o.ref, opts))
	if err != nil {
		return gestalt.ObjectMeta{}, nil, err
	}
	return result.Meta, result.Body, nil
}

func (o hostS3Object) Bytes(ctx context.Context, opts *gestalt.ReadOptions) ([]byte, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).Bytes(ctx, opts)
}

func (o hostS3Object) Text(ctx context.Context, opts *gestalt.ReadOptions) (string, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).Text(ctx, opts)
}

func (o hostS3Object) JSON(ctx context.Context, opts *gestalt.ReadOptions) (any, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).JSON(ctx, opts)
}

func (o hostS3Object) Write(ctx context.Context, body io.Reader, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.client.WriteObject(ctx, writeRequest(o.ref, body, opts))
}

func (o hostS3Object) WriteBytes(ctx context.Context, body []byte, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).WriteBytes(ctx, body, opts)
}

func (o hostS3Object) WriteString(ctx context.Context, body string, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).WriteString(ctx, body, opts)
}

func (o hostS3Object) WriteJSON(ctx context.Context, value any, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.client.Object(o.ref.Bucket, o.ref.Key).WriteJSON(ctx, value, opts)
}

func (o hostS3Object) Delete(ctx context.Context) error {
	return o.client.DeleteObject(ctx, o.ref)
}

func (o hostS3Object) Presign(ctx context.Context, opts *gestalt.PresignOptions) (gestalt.PresignResult, error) {
	return o.client.PresignObject(ctx, presignRequest(o.ref, opts))
}
