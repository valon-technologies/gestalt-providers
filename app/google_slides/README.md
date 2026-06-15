# Google Slides

Create and update Google Slides presentations.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  google_slides:
    source: github.com/valon-technologies/gestalt-providers/app/google_slides
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Slides OpenAPI specification. Exposes
operations for creating and getting presentations, batch-updating slides, and
retrieving page content and thumbnails.

Authenticates with Google OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  google_slides:
    source: github.com/valon-technologies/gestalt-providers/app/google_slides
    version: ...
    config:
      clientId: ${GOOGLE_SLIDES_CLIENT_ID}
      clientSecret: ${GOOGLE_SLIDES_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Google OAuth client ID for Google Slides.
- `clientSecret` (required): Google OAuth client secret for Google Slides.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/presentations`.

Operation surfaces: OpenAPI.

Representative operations include:

- `presentations.get`
- `presentations.create`
- `presentations.batchUpdate`
- `pages.get`
- `pages.getThumbnail`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `presentations.get` call:

```ts
await app.invoke("google_slides", "presentations.get", { presentationId: "presentation-id" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
