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

## Model Schema

`WriteModel` accepts YAML or JSON with one top-level `resource_types` map.
This v1 provider supports only direct relationships and action-to-relation
mapping.

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

Relation definitions may use either the explicit `subject_types` form shown
above or the shorthand list form:

```yaml
relations:
  viewer: [user]
```

Action definitions accept a single relation, a list of relations, or an
explicit `relations` field.
