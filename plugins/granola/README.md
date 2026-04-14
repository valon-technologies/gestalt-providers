# Granola

Read meeting notes, summaries, attendees, and transcripts from Granola.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  granola:
    source: github.com/valon-technologies/gestalt-providers/plugins/granola
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on Granola's public REST API. Exposes the note list
and note detail endpoints, including cursor-based pagination and optional
transcript retrieval.

Authenticates with a Granola API key using HTTP Bearer auth. Generate a key in
the Granola desktop app under Settings > API. Granola documents personal API
keys for Business and Enterprise workspaces, plus enterprise-scoped keys for
workspace admins.

Granola only returns notes that already have an AI summary and transcript.

## Documentation

- [Granola API Introduction](https://docs.granola.ai/introduction)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
