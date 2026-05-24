package fake

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

// ProviderClient implements the object-store test surface by calling gestalt.S3Provider directly.
type ProviderClient struct {
	Provider gestalt.S3Provider
}

func NewProviderClient(provider gestalt.S3Provider) ProviderClient {
	return ProviderClient{Provider: provider}
}

func (c ProviderClient) Close() error { return nil }

func (c ProviderClient) Object(bucket, key string) ProviderObject {
	return ProviderObject{provider: c.Provider, ref: gestalt.ObjectRef{Bucket: bucket, Key: key}}
}

func (c ProviderClient) HeadObject(ctx context.Context, ref gestalt.ObjectRef) (gestalt.ObjectMeta, error) {
	meta, err := c.Provider.HeadObject(ctx, ref)
	return meta, gestalt.MapProviderClientError(err)
}

func (c ProviderClient) ReadObject(ctx context.Context, req gestalt.ReadRequest) (gestalt.ReadResult, error) {
	opts := &gestalt.ReadOptions{
		Range:             req.Range,
		IfMatch:           req.IfMatch,
		IfNoneMatch:       req.IfNoneMatch,
		IfModifiedSince:   req.IfModifiedSince,
		IfUnmodifiedSince: req.IfUnmodifiedSince,
	}
	meta, body, err := c.Provider.ReadObject(ctx, req.Ref, opts)
	if err != nil {
		return gestalt.ReadResult{}, gestalt.MapProviderClientError(err)
	}
	return gestalt.ReadResult{Meta: meta, Body: &eofAfterCloseReader{ReadCloser: body}}, nil
}

func (c ProviderClient) WriteObject(ctx context.Context, req gestalt.WriteRequest) (gestalt.ObjectMeta, error) {
	opts := &gestalt.WriteOptions{
		ContentType:        req.ContentType,
		CacheControl:       req.CacheControl,
		ContentDisposition: req.ContentDisposition,
		ContentEncoding:    req.ContentEncoding,
		ContentLanguage:    req.ContentLanguage,
		Metadata:           req.Metadata,
		IfMatch:            req.IfMatch,
		IfNoneMatch:        req.IfNoneMatch,
	}
	meta, err := c.Provider.WriteObject(ctx, req.Ref, req.Body, opts)
	return meta, gestalt.MapProviderClientError(err)
}

func (c ProviderClient) DeleteObject(ctx context.Context, ref gestalt.ObjectRef) error {
	return gestalt.MapProviderClientError(c.Provider.DeleteObject(ctx, ref))
}

func (c ProviderClient) ListObjects(ctx context.Context, opts gestalt.ListOptions) (gestalt.ListPage, error) {
	page, err := c.Provider.ListObjects(ctx, opts)
	return page, gestalt.MapProviderClientError(err)
}

func (c ProviderClient) CopyObject(ctx context.Context, req gestalt.CopyRequest) (gestalt.ObjectMeta, error) {
	meta, err := c.Provider.CopyObject(ctx, req.Source, req.Destination, &gestalt.CopyOptions{
		IfMatch:     req.IfMatch,
		IfNoneMatch: req.IfNoneMatch,
	})
	return meta, gestalt.MapProviderClientError(err)
}

func (c ProviderClient) PresignObject(ctx context.Context, req gestalt.PresignRequest) (gestalt.PresignResult, error) {
	result, err := c.Provider.PresignObject(ctx, req.Ref, &gestalt.PresignOptions{
		Method:             req.Method,
		Expires:            req.Expires,
		ContentType:        req.ContentType,
		ContentDisposition: req.ContentDisposition,
		Headers:            req.Headers,
	})
	return result, gestalt.MapProviderClientError(err)
}

type ProviderObject struct {
	provider gestalt.S3Provider
	ref      gestalt.ObjectRef
}

func (o ProviderObject) Stat(ctx context.Context) (gestalt.ObjectMeta, error) {
	meta, err := o.provider.HeadObject(ctx, o.ref)
	return meta, gestalt.MapProviderClientError(err)
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
		return gestalt.ObjectMeta{}, nil, gestalt.MapProviderClientError(err)
	}
	return meta, &eofAfterCloseReader{ReadCloser: body}, nil
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
	return meta, gestalt.MapProviderClientError(err)
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
	return gestalt.MapProviderClientError(o.provider.DeleteObject(ctx, o.ref))
}

func (o ProviderObject) Presign(ctx context.Context, opts *gestalt.PresignOptions) (gestalt.PresignResult, error) {
	result, err := o.provider.PresignObject(ctx, o.ref, opts)
	return result, gestalt.MapProviderClientError(err)
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
