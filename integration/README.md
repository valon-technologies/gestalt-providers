# integration

Cross-provider integration tests. This module is build-time-only: it produces
no provider package and isn't released.

## Why this exists

Some providers (currently `external_credentials/default` and
`workflow/indexeddb`) need a real `indexeddb` backend to exercise their full
behavior. Hosting those tests inside the provider's own module would force the
provider to declare an in-repo dependency on `indexeddb/relationaldb`, which
would in turn make CI fan a `relationaldb` change out to those providers.

The integration target lives in its own module so each provider stays
self-contained. CI runs the integration target as its own gated job:

```
cd integration && go test ./...
```

## Adding a new integration test

1. Add a `*_test.go` file under `integration/` (one file per provider being
   tested, by convention).
2. Use the standard external test package pattern (`package <provider>_test`).
3. If the provider being tested isn't already in `go.mod`, add it as both a
   `require` (with the sentinel pseudo-version) and a `replace` pointing at the
   provider's source dir.
4. Run `go mod tidy && go test ./...` to verify.
