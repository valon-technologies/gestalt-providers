# IndexedDB Authorization

Authorization provider backed by the host IndexedDB service exposed through
`gestalt.IndexedDB(...)`.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
providers:
  indexeddb:
    default:
      source:
        ref: github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb
        version: 0.0.1-alpha.1

  authorization:
    indexeddb:
      source:
        ref: github.com/valon-technologies/gestalt-providers/authorization/indexeddb
        version: 0.0.1-alpha.1
      config:
        indexeddb: default

server:
  providers:
    indexeddb: default
    authorization: indexeddb
```

Configuration fields:

- `indexeddb`: Optional named host IndexedDB provider to connect to. Omit it to
  use the default IndexedDB socket.

## Authorization Model

`WriteModel` now accepts a typed `AuthorizationModel`. This provider supports
only direct relationships and action-to-relation mapping. The logical model
shape is:

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
