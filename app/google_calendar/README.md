# Google Calendar

Read and manage Google calendars and events.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  google_calendar:
    source: github.com/valon-technologies/gestalt-providers/app/google_calendar
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Calendar OpenAPI specification. Exposes
operations for listing calendars, and listing, creating, updating, deleting, and
quick-adding events.

Authenticates with Google OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  google_calendar:
    source: github.com/valon-technologies/gestalt-providers/app/google_calendar
    version: ...
    config:
      clientId: ${GOOGLE_CALENDAR_CLIENT_ID}
      clientSecret: ${GOOGLE_CALENDAR_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Google OAuth client ID for Google Calendar.
- `clientSecret` (required): Google OAuth client secret for Google Calendar.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/calendar`.

Operation surfaces: OpenAPI.

Representative operations include:

- `events.list`
- `events.quickAdd`
- `calendarList.list`
- `calendarList.get`
- `events.get`
- `events.insert`
- `events.update`
- `events.delete`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `events.list` call:

```ts
await app.invoke("google_calendar", "events.list", { calendarId: "primary", maxResults: 10 });
```

Example `events.quickAdd` call:

```ts
await app.invoke("google_calendar", "events.quickAdd", { calendarId: "primary", text: "Team sync tomorrow at 10am" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
