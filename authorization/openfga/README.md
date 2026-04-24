# OpenFGA Authorization

Authorization provider backed by an OpenFGA store.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
providers:
  authorization:
    openfga:
      source:
        ref: github.com/valon-technologies/gestalt-providers/authorization/openfga
        version: 0.0.1-alpha.1
      config:
        apiUrl: http://127.0.0.1:8080
        storeId: 01JSAMPLE7H7T4M5B7KX2K0N8

server:
  providers:
    authorization: openfga
```

Supported config fields:

- `apiUrl`: Required OpenFGA HTTP API base URL.
- `storeId`: Required OpenFGA store ID.
- `apiToken`: Optional bearer token for pre-shared-key style auth.
- `clientId`: Optional client-credentials client ID.
- `clientSecret`: Optional client-credentials client secret.
- `apiTokenIssuer`: Optional client-credentials token issuer or token URL.
- `apiAudience`: Optional client-credentials audience.
- `scopes`: Optional space-delimited client-credentials scopes.

Authentication modes:

- Omit all auth fields for unauthenticated local or trusted deployments.
- Set `apiToken` for bearer-token auth.
- Set `clientId`, `clientSecret`, `apiTokenIssuer`, and either `apiAudience` or `scopes` for OAuth2 client credentials.

## Authorization Model

`WriteModel` accepts the same typed `AuthorizationModel` shape used by the IndexedDB provider:

```yaml
version: 1
resource_types:
  document:
    relations:
      viewer:
        subject_types: [user]
      editor:
        subject_types: [user]
    actions:
      read: [viewer, editor]
      write: [editor]
```

The typed equivalent is:

```go
&proto.AuthorizationModel{
  Version: 1,
  ResourceTypes: []*proto.AuthorizationModelResourceType{{
    Name: "document",
    Relations: []*proto.AuthorizationModelRelation{
      {Name: "viewer", SubjectTypes: []string{"user"}},
      {Name: "editor", SubjectTypes: []string{"user"}},
    },
    Actions: []*proto.AuthorizationModelAction{
      {Name: "read", Relations: []string{"viewer", "editor"}},
      {Name: "write", Relations: []string{"editor"}},
    },
  }},
}
```

OpenFGA stores the typed model as computed relations under schema `1.1`, so:

- `WriteModel` is content-idempotent from the Gestalt side even though OpenFGA models are immutable.
- Gestalt model IDs are content digests. Re-writing an existing model reactivates it and returns the same model ID even if the provider needs to write a fresh OpenFGA authorization model under the hood.
- The latest OpenFGA authorization model is treated as the active model for `Evaluate` and `Search*`.
- `WriteRelationships` pins writes to the requested or active OpenFGA authorization model ID.

## Provider Usage

Serve the provider from Go:

```go
package main

import (
  "context"
  "log"

  gestalt "github.com/valon-technologies/gestalt/sdk/go"
  openfgaprovider "github.com/valon-technologies/gestalt-providers/authorization/openfga"
)

func main() {
  if err := gestalt.ServeAuthorizationProvider(context.Background(), openfgaprovider.New()); err != nil {
    log.Fatal(err)
  }
}
```

Use it directly in-process during tests or local development:

```go
ctx := context.Background()

provider := openfgaprovider.New()
if err := provider.Configure(ctx, "authorization", map[string]any{
  "apiUrl":  "http://127.0.0.1:8080",
  "storeId": "01JSAMPLE7H7T4M5B7KX2K0N8",
}); err != nil {
  return err
}
defer provider.Close()

decision, err := provider.Evaluate(ctx, &proto.AccessEvaluationRequest{
  Subject:  &proto.Subject{Type: "user", Id: "alice"},
  Action:   &proto.Action{Name: "read"},
  Resource: &proto.Resource{Type: "document", Id: "doc-1"},
})
if err != nil {
  return err
}
if decision.GetAllowed() {
  // ...
}
```

## Notes

- `SearchResources`, `SearchSubjects`, and `SearchActions` are implemented on top of OpenFGA `ListObjects`, `ListUsers`, and relation checks, then paginated locally to preserve the Gestalt API shape.
- `ReadRelationships` returns only directly stored tuples, matching OpenFGA `Read`.
- Relationship, subject, and resource `properties` on `WriteRelationships` are rejected with `INVALID_ARGUMENT` because OpenFGA tuples do not carry arbitrary property payloads.
- OpenFGA `ListUsers` must be enabled on the server for subject search support, for example: `openfga run --experimentals=enable-list-users`.
