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
        version: 0.0.1-alpha.2

  authorization:
    indexeddb:
      source:
        ref: github.com/valon-technologies/gestalt-providers/authorization/indexeddb
        version: 0.0.1-alpha.2
      config:
        indexeddb: default

server:
  providers:
    indexeddb: default
    authorization: indexeddb
```

Configuration fields:

- `indexeddb`: Optional named host IndexedDB provider binding. Omit it to use
  the default IndexedDB binding on the unified host-service socket.

## Authorization Model

`WriteModel` accepts a typed `AuthorizationModel`. The provider supports direct
relationships, generalized relationship targets, action-to-relation mapping, and
Zanzibar-style rewrites for `this`, `computed_userset`, `tuple_to_userset`, and
`union`. The logical model shape is:

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
