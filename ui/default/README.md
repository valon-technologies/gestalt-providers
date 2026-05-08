# Default UI

Default Gestalt UI bundle served at `/`. The same published artifact also
contains the provider-owned admin shell under `admin/` for `gestaltd`'s
`/admin` surface.

## Overview

This package ships two static surfaces in a single bundle:

- **Root UI** (`/`) — a Next.js app (App Router, `output: "export"`) that
  hosts the user-facing workspace: dashboard, plugins, authorization,
  workflows, agents, and in-app docs.
- **Admin shell** (`admin/`) — the static admin surface that `gestaltd`
  serves at `/admin`. It is bundled into the same `out/` artifact during
  release so a single UI provider covers both surfaces.

Both surfaces are static assets resolved by `gestaltd` at runtime. There is
no separate Node server in production.

## Surfaces

The root UI exposes the following routes (see `src/app/`):

| Path                | Purpose                                                |
| ------------------- | ------------------------------------------------------ |
| `/`                 | Dashboard with counts for plugins, tokens, workflows, and agent sessions. |
| `/login`            | Login flow when an authentication provider is configured. |
| `/authorization`    | API tokens and the authorization browser.              |
| `/tokens`           | Token management for the current identity.            |
| `/identities`       | Managed identities and their tokens.                   |
| `/integrations`     | Connect, configure, and inspect plugins.               |
| `/workflows`        | Schedules, event triggers, and recent runs.            |
| `/agents`           | Agent sessions (only when the auth provider exposes the `agent` feature). |
| `/docs`             | In-app user-facing docs. The full content lives in `src/app/docs/`. |

The admin shell mounts at `/admin` and is served from `public/admin/`. It is
built and packaged independently of the Next app and does not require a
separate provider entry — `gestaltd` auto-discovers it from the bundle.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
server:
  admin:
    # Optional. When omitted, gestaltd auto-discovers admin/ from the root UI bundle.
    ui: default

providers:
  ui:
    default:
      source: github.com/valon-technologies/gestalt-providers/ui/default
      path: /
```

`path` controls where the bundle mounts. Use `/` to serve the bundle as the
root UI; mount additional UI providers (for example `ui/github`) at
sub-paths such as `/github`.

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

If you serve this static bundle behind a Content Security Policy, allow
`data:` in `img-src` for provider icons.

## Local Development

`./dev.sh` builds the static export and serves it through a local `gestaltd`
so routing and packaging match production. It expects a sibling checkout of
the `gestalt` repository at `../../../gestalt`. Override the location with
`GESTALT_CHECKOUT=/path/to/gestalt` if your layout differs.

```sh
# Auto-generates ~/.gestaltd/config.yaml on first run
./dev.sh

# Use a custom config
./dev.sh gestalt.local.yaml

# Run on a non-default port (creates a per-port dev config)
API_PORT=9090 ./dev.sh
```

The script:

- Installs npm dependencies if `node_modules` is missing.
- Loads environment variables from `../../.env` if present.
- Runs `npm run build` to produce `out/`.
- Builds and runs `gestaltd` from `$GESTALT_CHECKOUT/gestaltd`, pointing
  `GESTALTD_CLIENT_UI_DIR` at this package's `out/` directory.
- Waits for `http://localhost:$API_PORT/health` and prints the ready URL.

Dev mode disables auth in the auto-generated config — re-run the script
after editing UI source so the export refreshes. For a faster inner loop on
the Next app alone, use `npm run dev:next` (no admin shell, no `gestaltd`).

Useful checks:

```sh
npm run typecheck   # tsc --noEmit
npm run lint        # eslint .
npm run check       # typecheck + lint
```

## Build & Release

This package publishes the default Gestalt UI as a `ui` bundle.

`gestaltd provider release` runs the package's `release.build` recipe (see
`manifest.yaml`), which executes `./build.sh`:

```sh
npm ci
npm run build
```

`npm run build` runs `next build` with `output: "export"` (see
`next.config.mjs`), producing the static export under `out/`. The release
flow bundles the static admin shell into `out/` and packages everything
according to `spec.assetRoot: out`.

By default the build looks for a sibling checkout at `../../../gestalt` from
this package directory. Set `GESTALT_CHECKOUT=/path/to/gestalt` if your
local layout is different.

Tag releases with the standard `<kind>/<name>/v<version>` format from the
repository root, e.g. `ui/default/v0.0.1-alpha.34`. See
[Releasing](https://gestaltd.ai/providers/releasing) for the CI pipeline.

## End-to-End Tests

Playwright specs live in `e2e/`. By default the Playwright config boots
`./dev.sh` (or `npx serve out` in CI) and points the browser at
`http://localhost:$API_PORT`.

```sh
npm run test:e2e         # headless
npm run test:e2e:headed  # with browser UI
npm run test:e2e:ui      # Playwright UI mode
```

Most specs (`*-mock.spec.ts`) run against mocked API responses and do not
require a live `gestaltd`. The non-mock specs (`auth.spec.ts`,
`integrations.spec.ts`, `tokens.spec.ts`) need a running backend.

To run the suite against an existing backend, set `GESTALT_BASE_URL` (or
`PLAYWRIGHT_BASE_URL`):

```sh
GESTALT_BASE_URL=https://staging.example.com npm run test:e2e
```

## Theming

The UI ships light, dark, and system themes. The active mode is read from
`localStorage` under the `theme` key (`light`, `dark`, or `system`); the
inline script in `src/app/layout.tsx` applies the right class before the
first paint to avoid a flash of the wrong theme.

Fonts are loaded locally from `public/fonts/` via `next/font/local` — no
network font requests are made at runtime.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
