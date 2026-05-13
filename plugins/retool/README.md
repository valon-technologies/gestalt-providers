# Retool

Gestalt plugin for Retool Cloud. The REST surface is generated from Retool's official OpenAPI spec at `https://api.retool.com/api/v2/spec` and excludes operations marked deprecated in that spec. The source-backed operations wrap the official Retool CLI where the CLI exposes useful capabilities that are not already covered by the OpenAPI surface.

## Authentication

Retool documents Bearer-token authentication for the Retool API. Create a scoped access token in Retool under **Settings > Retool API**, then connect the default plugin connection with that token.

`apps.exportApp` uses Retool CLI session cookies because the official CLI export path calls Retool's browser-session endpoints, not the public API token endpoint. Connect the `cliSession` connection with:

- `origin`: your Retool org URL, for example `https://your-org.retool.com`
- `access_token`: the browser cookie named `accessToken` from the same Retool org session
- `xsrf_token`: the browser cookie named `xsrfToken` from the same Retool org session

Invoke `apps.exportApp` with `_connection=cliSession` so Gestalt resolves those manual credentials for the operation. Those fields mirror the official CLI's cookie login state. The CLI source stores them as `origin`, `accessToken`, and `xsrf`, then sends `x-xsrf-token` and `cookie: accessToken=...` headers when exporting an app.

```bash
gestalt plugin invoke retool apps.exportApp \
  --connection cliSession \
  -p app_name="Ops dashboard"
```

The CLI operations invoke the official `retool-cli` npm package. If a `retool` binary is already available on `PATH`, the provider uses it. Otherwise it falls back to `npx --yes --package retool-cli@1.0.29 -- retool`, which installs and runs the audited package version through npm at runtime and requires Node.js and npm. Set `RETOOL_CLI_COMMAND` to override the command, for example `RETOOL_CLI_COMMAND=/opt/bin/retool`.

`terraform.generateConfiguration` uses the default Retool API token connection and requires the Retool host domain, for example `example.retool.com`.

## REST API coverage

All stable, documented Retool Cloud API operations from the official spec are listed explicitly in `manifest.yaml` under `allowedOperations`. Pagination is configured for Retool's documented `data`, `has_more`, and `next_token` response shape.

## CLI coverage

The plugin exposes CLI operations only for documented Retool CLI capabilities that are not already represented by the OpenAPI spec and make sense in a non-interactive plugin context: `apps.exportApp`, `customComponentLibraries.cloneRepository`, and `terraform.generateConfiguration`. `apps.exportApp` is included even though it needs browser-session cookies because the CLI can be given those credentials through an ephemeral keyring shim without writing native keychain state.

It intentionally does not expose CLI commands that overlap REST operations (`apps --list`, `apps --delete`, `workflows --list`, `workflows --delete`), local account/session commands (`login`, `logout`, `signup`, `whoami`, and telemetry toggles), or CLI commands that are interactive or broad composite mutations (`apps --create`, `apps --create-from-table`, `db`, `scaffold`, and `rpc` variants).

CLI operations run in an isolated temporary working directory and return stdout, stderr, exit code, parsed JSON when stdout is JSON, and generated working-directory file contents. Temporary CLI home files, npm caches, Git metadata, and dependency directories are not returned so authentication state, npm cache files, and cloned repository internals are not leaked in operation responses. Secret command arguments and Retool session cookie values are redacted from returned command metadata, stdout, and stderr.

## Tests

Tests are mocked only. They validate operation coverage, generated OpenAPI metadata, CLI argv construction, and file capture without requiring Retool credentials or a live Retool workspace.
