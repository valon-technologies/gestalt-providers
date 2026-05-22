# Ashby

Candidates, applications, jobs, offers, interviews, departments, locations, users, and reports.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  ashby:
    source: github.com/valon-technologies/gestalt-providers/apps/ashby
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on a local OpenAPI specification. Exposes operations
for managing the full recruiting lifecycle including candidates, applications,
candidate file URLs, jobs, offers, interviews, interview stages, departments,
locations, users, and reports. Supports cursor-based pagination.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  ashby:
    source: github.com/valon-technologies/gestalt-providers/apps/ashby
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `user`.

Operation surfaces: OpenAPI.

Representative operations include:

- `candidate.list`
- `candidate.info`
- `candidate.search`
- `candidate.create`
- `candidate.update`
- `candidate.createNote`
- `application.list`
- `application.info`
- `application.create`
- `application.changeStage`
- `applicationFeedback.submit`
- `feedbackFormDefinition.list`
- `feedbackFormDefinition.info`
- `interviewBriefing.info`

- Ashby API keys use HTTP Basic authentication as described by Ashby; create a key in Ashby admin settings.
- When creating the default connection, provide the Ashby API key for the HTTP
  Basic credential. If your connection UI separates username and password, use
  the API key as the Basic username and leave the password empty unless Ashby
  instructs otherwise.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: ashby
        operation: candidate.list
```

Example `candidate.list` call:

```ts
await invoker.invoke("ashby", "candidate.list", { limit: 25 });
```

Example `candidate.info` call:

```ts
await invoker.invoke("ashby", "candidate.info", { id: "candidate-id" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
