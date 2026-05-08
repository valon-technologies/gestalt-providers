# Default UI

Default Gestalt UI bundle served at `/`. The same published artifact also
contains the provider-owned admin shell under `admin/` for `gestaltd`'s
`/admin` surface.

## Overview

`ui/default` is the reference UI bundle that ships with Gestalt. It is a
[Next.js](https://nextjs.org/) application exported as static assets and
served by `gestaltd` as a `ui` provider.

A single published artifact backs two surfaces:

- **`/`** — the end-user UI: agents, workflows, integrations, identities,
  authorization, tokens, login, and the in-app documentation shell under
  `/docs`.
- **`/admin`** — the provider-owned admin shell. `gestaltd` auto-discovers
  the `admin/` directory inside this bundle when `server.admin.ui` is
  omitted, so most deployments do not need to configure it explicitly.

The in-app docs at `/docs` mirror the public docs site and cover
`getting-started`, `connect`, `invoke`, `mcp`, `tokens`, `workflows`, and
`troubleshooting`.

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

Set `path` to mount the bundle somewhere other than `/` (for example
`/app`). The admin shell is always served from `/admin` and is not affected
by `path`.

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

### Content Security Policy

If you serve this static bundle behind a Content Security Policy, allow
`data:` in `img-src` for provider icons. Inline SVGs and provider-supplied
icons are rendered as data URLs, so a strict `img-src 'self'` will break
them.

## Repository layout

```
ui/default/
├── manifest.yaml          # Provider manifest consumed by gestaltd
├── package.json           # @gestalt/ui-default package
├── build.sh               # Production build entrypoint (gestaltd provider release)
├── dev.sh                 # Local dev script — builds + runs gestaltd
├── next.config.mjs        # Next.js config (static export)
├── tailwind.config.ts     # Tailwind config
├── playwright.config.ts   # Playwright config for e2e tests
├── public/                # Static assets copied into out/
├── shared/                # theme.css and assets shared across surfaces
├── src/
│   ├── app/               # Next.js App Router surfaces
│   │   ├── agents/
│   │   ├── auth/
│   │   ├── authorization/
│   │   ├── docs/          # In-app docs surface served at /docs
│   │   ├── identities/
│   │   ├── integrations/
│   │   ├── login/
│   │   ├── tokens/
│   │   └── workflows/
│   ├── components/        # Shared React components
│   ├── hooks/             # React hooks
│   ├── lib/               # API clients, helpers
│   └── types/             # Shared TypeScript types
└── e2e/                   # Playwright tests
```

## Local development

The dev workflow builds the static export and serves it through a local
`gestaltd` so routing and packaging match production.

### Prerequisites

- Node.js (matching `@types/node` major in `package.json`)
- Go (for building `gestaltd`)
- A sibling [`gestalt`](https://github.com/valon-technologies/gestalt)
  checkout at `../../../gestalt` from this directory, or set
  `GESTALT_CHECKOUT=/path/to/gestalt`.

### Run

```bash
./dev.sh                         # auto-generates ~/.gestaltd/config.yaml
./dev.sh gestalt.local.yaml      # use a custom config (relative to repo root)
API_PORT=9090 ./dev.sh           # run gestaltd on a different port
GESTALT_CHECKOUT=/path/to/gestalt ./dev.sh
```

`dev.sh`:

1. Installs npm dependencies if `node_modules/` is missing.
2. Sources `../../.env` if present.
3. Generates a minimal config under `~/.gestaltd-dev/api-${API_PORT}/`
   when no config is supplied and `API_PORT` is non-default.
4. Runs `npm run build` to produce `out/`.
5. Builds `gestaltd` from the sibling checkout.
6. Starts `gestaltd` with `GESTALTD_CLIENT_UI_DIR` pointed at `out/`.

The UI is served from `out/`, so re-run `./dev.sh` after UI changes. Auth
is disabled in the auto-generated dev config — do not point it at
production data.

If you want to iterate on the UI without `gestaltd` in the loop, you can
run the Next.js dev server directly:

```bash
npm run dev:next
```

Note that `dev:next` does not exercise the same routing or the admin
shell; use `./dev.sh` for end-to-end behavior.

## Build

This package publishes the default Gestalt UI as a `ui` bundle.

`gestaltd provider release` runs the package's `release.build` recipe
(`./build.sh`), which:

1. Runs `npm ci` for a reproducible install.
2. Runs `npm run build`, which builds the Next.js app as a static export
   and bundles the static admin shell into `out/`.

The `manifest.yaml` declares `spec.assetRoot: out`, so `out/` is what
`gestaltd` mounts at runtime.

By default the build looks for a sibling checkout at `../../../gestalt`
from this package directory. Set `GESTALT_CHECKOUT=/path/to/gestalt` if
your local layout is different.

## Testing

```bash
npm run typecheck       # tsc --noEmit
npm run lint            # eslint .
npm run check           # typecheck + lint
npm run test:e2e        # Playwright (headless)
npm run test:e2e:headed # Playwright (headed browser)
npm run test:e2e:ui     # Playwright UI mode
```

Playwright specs live under `e2e/`. They expect a running `gestaltd` —
the simplest setup is `./dev.sh` in one terminal and `npm run test:e2e`
in another.

## Customization

Most teams should consume this bundle as-is. If you need to ship a
customized UI, prefer one of:

1. **Theme overrides** via the standard Gestalt theming hooks rather
   than forking.
2. **A new `ui` provider** in this repository (see `ui/github` for a
   minimal example) that mounts at a different path and reuses pieces
   of `ui/default`.
3. **A fork** of this directory published from your own repo, with the
   `manifest.yaml` `source:` field updated to point at your fork.

When forking, keep the `manifest.yaml` `kind: ui` and `spec.assetRoot:
out` so `gestaltd` can mount it without extra configuration.

## Versioning & publishing

The published version is the `version` field in `manifest.yaml`
(currently mirrored in `package.json`). `gestaltd provider release`
publishes the bundle under the `source:` URL declared in the manifest:

```
github.com/valon-technologies/gestalt-providers/ui/default
```

Bump the manifest `version` (and `package.json` `version`) using
semver:

- **patch** — bug fixes, copy tweaks, dependency bumps with no
  user-visible behavior change.
- **minor** — new screens, new components, new opt-in behavior.
- **major** — removed routes, breaking layout changes, or breaking API
  expectations against `gestaltd`.

Pre-1.0 alpha versions (`0.0.1-alpha.N`) are the current default; bump
the `alpha.N` suffix for in-flight releases.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [Getting Started](https://gestaltd.ai/getting-started)
