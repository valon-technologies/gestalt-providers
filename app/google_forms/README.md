# Google Forms

Read and manage Google Forms and responses.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  google_forms:
    source: github.com/valon-technologies/gestalt-providers/app/google_forms
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Forms OpenAPI specification. Exposes
operations for getting, creating, and updating forms, and listing and getting
form responses.

Authenticates with Google OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  google_forms:
    source: github.com/valon-technologies/gestalt-providers/app/google_forms
    version: ...
    config:
      clientId: ${GOOGLE_FORMS_CLIENT_ID}
      clientSecret: ${GOOGLE_FORMS_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Google OAuth client ID for Google Forms.
- `clientSecret` (required): Google OAuth client secret for Google Forms.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/forms.body`, `https://www.googleapis.com/auth/forms.responses.readonly`.

Operation surfaces: OpenAPI.

Representative operations include:

- `forms.get`
- `responses.list`
- `forms.create`
- `forms.batchUpdate`
- `responses.list`
- `responses.get`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `forms.get` call:

```ts
await app.invoke("google_forms", "forms.get", { formId: "form-id" });
```

Example `responses.list` call:

```ts
await app.invoke("google_forms", "responses.list", { formId: "form-id" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
