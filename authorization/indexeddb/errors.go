package indexeddb

type providerError struct {
	err error
}

func (e providerError) Error() string {
	if e.err == nil {
		return "authorization: (nil)"
	}
	return "authorization: " + e.err.Error()
}

func (e providerError) Unwrap() error {
	return e.err
}

func newAuthorizationProviderError(err error) error {
	if err == nil {
		return nil
	}
	return providerError{err: err}
}
