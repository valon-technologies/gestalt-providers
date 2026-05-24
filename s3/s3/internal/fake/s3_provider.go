package fake

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

type ProviderS3Client struct {
	Provider gestalt.S3Provider
}

func NewProviderS3Client(provider gestalt.S3Provider) ProviderS3Client {
	return ProviderS3Client{Provider: provider}
}

func (c ProviderS3Client) Close() error { return nil }

func (c ProviderS3Client) Object(bucket, key string) ProviderObject {
	return ProviderObject{provider: c.Provider, ref: gestalt.ObjectRef{Bucket: bucket, Key: key}}
}

func (c ProviderS3Client) HeadObject(ctx context.Context, ref gestalt.ObjectRef) (gestalt.ObjectMeta, error) {
	meta, err := c.Provider.HeadObject(ctx, ref)
	return meta, MapClientError(err)
}

func (c ProviderS3Client) ReadObject(ctx context.Context, ref gestalt.ObjectRef, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error) {
	meta, body, err := c.Provider.ReadObject(ctx, ref, opts)
	if err != nil {
		return gestalt.ObjectMeta{}, nil, MapClientError(err)
	}
	return meta, &EOFAfterCloseReader{ReadCloser: body}, nil
}

func (c ProviderS3Client) WriteObject(ctx context.Context, ref gestalt.ObjectRef, body io.Reader, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	meta, err := c.Provider.WriteObject(ctx, ref, body, opts)
	return meta, MapClientError(err)
}

func (c ProviderS3Client) DeleteObject(ctx context.Context, ref gestalt.ObjectRef) error {
	return MapClientError(c.Provider.DeleteObject(ctx, ref))
}

func (c ProviderS3Client) ListObjects(ctx context.Context, opts gestalt.ListOptions) (gestalt.ListPage, error) {
	page, err := c.Provider.ListObjects(ctx, opts)
	return page, MapClientError(err)
}

func (c ProviderS3Client) CopyObject(ctx context.Context, source, destination gestalt.ObjectRef, opts *gestalt.CopyOptions) (gestalt.ObjectMeta, error) {
	meta, err := c.Provider.CopyObject(ctx, source, destination, opts)
	return meta, MapClientError(err)
}

func (c ProviderS3Client) PresignObject(ctx context.Context, ref gestalt.ObjectRef, opts *gestalt.PresignOptions) (gestalt.PresignResult, error) {
	result, err := c.Provider.PresignObject(ctx, ref, opts)
	return result, MapClientError(err)
}

type ProviderObject struct {
	provider gestalt.S3Provider
	ref      gestalt.ObjectRef
}

func (o ProviderObject) Stat(ctx context.Context) (gestalt.ObjectMeta, error) {
	meta, err := o.provider.HeadObject(ctx, o.ref)
	return meta, MapClientError(err)
}

func (o ProviderObject) Exists(ctx context.Context) (bool, error) {
	_, err := o.Stat(ctx)
	if err == nil {
		return true, nil
	}
	if err == gestalt.ErrS3NotFound {
		return false, nil
	}
	return false, err
}

func (o ProviderObject) Stream(ctx context.Context, opts *gestalt.ReadOptions) (gestalt.ObjectMeta, io.ReadCloser, error) {
	meta, body, err := o.provider.ReadObject(ctx, o.ref, opts)
	if err != nil {
		return gestalt.ObjectMeta{}, nil, MapClientError(err)
	}
	return meta, &EOFAfterCloseReader{ReadCloser: body}, nil
}

func (o ProviderObject) Bytes(ctx context.Context, opts *gestalt.ReadOptions) ([]byte, error) {
	_, body, err := o.Stream(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()
	return io.ReadAll(body)
}

func (o ProviderObject) Text(ctx context.Context, opts *gestalt.ReadOptions) (string, error) {
	data, err := o.Bytes(ctx, opts)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (o ProviderObject) JSON(ctx context.Context, opts *gestalt.ReadOptions) (any, error) {
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

func (o ProviderObject) Write(ctx context.Context, body io.Reader, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	meta, err := o.provider.WriteObject(ctx, o.ref, body, opts)
	return meta, MapClientError(err)
}

func (o ProviderObject) WriteBytes(ctx context.Context, body []byte, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.Write(ctx, bytes.NewReader(body), opts)
}

func (o ProviderObject) WriteString(ctx context.Context, body string, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
	return o.WriteBytes(ctx, []byte(body), opts)
}

func (o ProviderObject) WriteJSON(ctx context.Context, value any, opts *gestalt.WriteOptions) (gestalt.ObjectMeta, error) {
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

func (o ProviderObject) Delete(ctx context.Context) error {
	return MapClientError(o.provider.DeleteObject(ctx, o.ref))
}

func (o ProviderObject) Presign(ctx context.Context, opts *gestalt.PresignOptions) (gestalt.PresignResult, error) {
	result, err := o.provider.PresignObject(ctx, o.ref, opts)
	return result, MapClientError(err)
}

type EOFAfterCloseReader struct {
	io.ReadCloser
	closed bool
}

func (r *EOFAfterCloseReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.EOF
	}
	return r.ReadCloser.Read(p)
}

func (r *EOFAfterCloseReader) Close() error {
	r.closed = true
	return r.ReadCloser.Close()
}

func MapClientError(err error) error {
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
