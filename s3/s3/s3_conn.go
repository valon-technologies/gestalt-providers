package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/aws/smithy-go"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type s3Conn interface {
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

var connectS3 = func(provider *Provider) (s3Conn, error) {
	if provider == nil {
		client, err := gestalt.S3()
		if err != nil {
			return nil, err
		}
		return sdkS3Conn{client}, nil
	}
	return providerS3Conn{provider: provider}, nil
}

type sdkS3Conn struct {
	*gestalt.S3Client
}

func (c sdkS3Conn) Object(bucket, key string) s3Object {
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

type providerS3Conn struct {
	provider *Provider
}

func (c providerS3Conn) Close() error { return nil }

func (c providerS3Conn) Object(bucket, key string) s3Object {
	return providerS3Object{provider: c.provider, ref: gestalt.ObjectRef{Bucket: bucket, Key: key}}
}

func (c providerS3Conn) HeadObject(ctx context.Context, ref gestalt.ObjectRef) (gestalt.ObjectMeta, error) {
	meta, err := c.provider.HeadObject(ctx, ref)
	return meta, providerClientErr(err)
}

func (c providerS3Conn) ReadObject(ctx context.Context, ref gestalt.ObjectRef, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error) {
	meta, body, err := c.provider.ReadObject(ctx, ref, opts)
	if err != nil {
		return gestalt.ObjectMeta{}, nil, providerClientErr(err)
	}
	return meta, &eofAfterCloseReader{ReadCloser: body}, nil
}

func (c providerS3Conn) WriteObject(ctx context.Context, ref gestalt.ObjectRef, body io.Reader, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	meta, err := c.provider.WriteObject(ctx, ref, body, opts)
	return meta, providerClientErr(err)
}

func (c providerS3Conn) DeleteObject(ctx context.Context, ref gestalt.ObjectRef) error {
	return providerClientErr(c.provider.DeleteObject(ctx, ref))
}

func (c providerS3Conn) ListObjects(ctx context.Context, opts gestalt.ListOptions) (gestalt.ListPage, error) {
	page, err := c.provider.ListObjects(ctx, opts)
	return page, providerClientErr(err)
}

func (c providerS3Conn) CopyObject(ctx context.Context, source, destination gestalt.ObjectRef, opts *gestalt.CopyOptions) (gestalt.ObjectMeta, error) {
	meta, err := c.provider.CopyObject(ctx, source, destination, opts)
	return meta, providerClientErr(err)
}

func (c providerS3Conn) PresignObject(ctx context.Context, ref gestalt.ObjectRef, opts *gestalt.PresignOptions) (gestalt.PresignResult, error) {
	result, err := c.provider.PresignObject(ctx, ref, opts)
	return result, providerClientErr(err)
}

type providerS3Object struct {
	provider *Provider
	ref      gestalt.ObjectRef
}

func (o providerS3Object) Stat(ctx context.Context) (gestalt.ObjectMeta, error) {
	meta, err := o.provider.HeadObject(ctx, o.ref)
	return meta, providerClientErr(err)
}

func (o providerS3Object) Exists(ctx context.Context) (bool, error) {
	_, err := o.Stat(ctx)
	if err == nil {
		return true, nil
	}
	if err == gestalt.ErrS3NotFound {
		return false, nil
	}
	return false, err
}

func (o providerS3Object) Stream(ctx context.Context, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error) {
	meta, body, err := o.provider.ReadObject(ctx, o.ref, opts)
	if err != nil {
		return gestalt.ObjectMeta{}, nil, providerClientErr(err)
	}
	return meta, &eofAfterCloseReader{ReadCloser: body}, nil
}

func (o providerS3Object) Bytes(ctx context.Context, opts *gestalt.ReadOptions) ([]byte, error) {
	_, body, err := o.Stream(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()
	return io.ReadAll(body)
}

func (o providerS3Object) Text(ctx context.Context, opts *gestalt.ReadOptions) (string, error) {
	data, err := o.Bytes(ctx, opts)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (o providerS3Object) JSON(ctx context.Context, opts *gestalt.ReadOptions) (any, error) {
	data, err := o.Bytes(ctx, opts)
	if err != nil {
		return nil, err
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func (o providerS3Object) Write(ctx context.Context, body io.Reader, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	meta, err := o.provider.WriteObject(ctx, o.ref, body, opts)
	return meta, providerClientErr(err)
}

func (o providerS3Object) WriteBytes(ctx context.Context, body []byte, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.Write(ctx, bytes.NewReader(body), opts)
}

func (o providerS3Object) WriteString(ctx context.Context, body string, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.WriteBytes(ctx, []byte(body), opts)
}

func (o providerS3Object) WriteJSON(ctx context.Context, value any, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return gestalt.ObjectMeta{}, err
	}
	if opts == nil {
		opts = &gestalt.WriteOptions{ContentType: "application/json"}
	} else if opts.ContentType == "" {
		opts.ContentType = "application/json"
	}
	return o.WriteBytes(ctx, body, opts)
}

func (o providerS3Object) Delete(ctx context.Context) error {
	return providerClientErr(o.provider.DeleteObject(ctx, o.ref))
}

func (o providerS3Object) Presign(ctx context.Context, opts *gestalt.PresignOptions) (gestalt.PresignResult, error) {
	result, err := o.provider.PresignObject(ctx, o.ref, opts)
	return result, providerClientErr(err)
}

type eofAfterCloseReader struct {
	io.ReadCloser
	closed bool
}

func (r *eofAfterCloseReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.EOF
	}
	return r.ReadCloser.Read(p)
}

func (r *eofAfterCloseReader) Close() error {
	r.closed = true
	return r.ReadCloser.Close()
}

func providerClientErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gestalt.ErrS3NotFound) {
		return err
	}
	if errors.Is(err, gestalt.ErrS3PreconditionFailed) {
		return err
	}
	if errors.Is(err, gestalt.ErrS3InvalidRange) {
		return err
	}
	if isS3NotFound(err) {
		return gestalt.ErrS3NotFound
	}
	if isS3PreconditionFailed(err) {
		return gestalt.ErrS3PreconditionFailed
	}
	if isS3InvalidRange(err) {
		return gestalt.ErrS3InvalidRange
	}
	if strings.Contains(err.Error(), "expires must be") {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if strings.Contains(err.Error(), "s3: invalid range") {
		return gestalt.ErrS3InvalidRange
	}
	return err
}

func isS3NotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket", "NoSuchVersion":
			return true
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "StatusCode: 404")
}

func isS3PreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "PreconditionFailed"
	}
	msg := err.Error()
	return strings.Contains(msg, "PreconditionFailed") ||
		strings.Contains(msg, "NotModified") ||
		strings.Contains(msg, "StatusCode: 412") ||
		strings.Contains(msg, "StatusCode: 304")
}

func isS3InvalidRange(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "InvalidRange", "InvalidArgument":
			return strings.Contains(apiErr.ErrorMessage(), "range")
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "s3: invalid range") ||
		strings.Contains(msg, "InvalidRange") ||
		strings.Contains(msg, "StatusCode: 416")
}
