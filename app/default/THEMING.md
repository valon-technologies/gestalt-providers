# Theming

`app/default` is a public, tenant-neutral bundle. It ships a complete generic
fallback and receives an optional tenant stylesheet at runtime; it never
imports a tenant palette, font, component registry, filesystem path, or build
dependency.

The browser always requests the relative URL `theme.css`. At a root mount that
is `/theme.css`; at `/portal/` it is `/portal/theme.css`. This is a delivery
contract: the hosting server selects and serves that stylesheet from deployment
configuration. A conforming server reserves the endpoint even when a
deployment has no theme, returning an empty `text/css` response so the generic
fallback renders normally.

## Public contract

The contract is defined by [`shared/theme.css`](shared/theme.css) and mirrored
as machine-readable names in [`ui-core.contract.json`](ui-core.contract.json).
It is deliberately shadcn-shaped:

| Group | Tokens |
| --- | --- |
| Core surfaces | `background`, `foreground`, `card`, `card-foreground`, `popover`, `popover-foreground` |
| Actions | `primary`, `primary-foreground`, `secondary`, `secondary-foreground`, `accent`, `accent-foreground`, `destructive`, `destructive-foreground` |
| Supporting UI | `muted`, `muted-foreground`, `border`, `input`, `ring` |
| Status extension | `success`, `success-foreground`, `warning`, `warning-foreground`, `info`, `info-foreground` |
| Geometry and type | `radius`, `ui-font-sans`, `ui-font-display`, `ui-font-mono`, `heading-weight` |

All defaults live under `:where(:root)` and `:where(.dark)`, which have zero
specificity. A tenant can therefore use ordinary `:root` / `.dark`
declarations and win regardless of when Vite emits its application CSS.

Components consume only semantic Tailwind utilities such as `bg-card`,
`text-muted-foreground`, `border-border`, `bg-primary`, and
`text-destructive`. Raw palette utilities do not belong in public component
code.

## Tenant stylesheet

A tenant may keep private palette constants and licensed font declarations
inside its own deployment repository, then map them into every public role.
The public bundle never needs to know their names. This is a complete template:
keep the semantic mapping stable, and change only the private values.

```css
@font-face {
  font-family: Tenant Sans;
  src: url("theme/fonts/tenant-sans.woff2") format("woff2");
  font-display: swap;
}

:root,
.dark {
  --background: var(--tenant-surface);
  --foreground: var(--tenant-ink);
  --card: var(--tenant-surface-raised);
  --card-foreground: var(--tenant-ink);
  --popover: var(--tenant-surface-raised);
  --popover-foreground: var(--tenant-ink);
  --primary: var(--tenant-action);
  --primary-foreground: var(--tenant-on-action);
  --secondary: var(--tenant-soft);
  --secondary-foreground: var(--tenant-soft-ink);
  --muted: var(--tenant-soft);
  --muted-foreground: var(--tenant-ink-muted);
  --accent: var(--tenant-soft);
  --accent-foreground: var(--tenant-soft-ink);
  --destructive: var(--tenant-danger);
  --destructive-foreground: var(--tenant-on-danger);
  --border: var(--tenant-rule);
  --input: var(--tenant-rule);
  --ring: var(--tenant-action);
  --success: var(--tenant-success);
  --success-foreground: var(--tenant-success-ink);
  --warning: var(--tenant-warning);
  --warning-foreground: var(--tenant-warning-ink);
  --info: var(--tenant-info);
  --info-foreground: var(--tenant-info-ink);
  --radius: 0.5rem;
  --ui-font-sans: Tenant Sans, ui-sans-serif, system-ui, sans-serif;
  --ui-font-display: Tenant Serif, ui-serif, Georgia, serif;
  --ui-font-mono: Tenant Mono, ui-monospace, monospace;
  --heading-weight: 500;
}

:root {
  --tenant-surface: oklch(99% 0.01 90);
  --tenant-surface-raised: oklch(100% 0 0);
  --tenant-ink: oklch(23% 0.02 52);
  --tenant-ink-muted: oklch(48% 0.02 52);
  --tenant-soft: oklch(95% 0.02 90);
  --tenant-soft-ink: oklch(28% 0.02 52);
  --tenant-action: oklch(56% 0.16 255);
  --tenant-on-action: white;
  --tenant-danger: oklch(57% 0.2 28);
  --tenant-on-danger: white;
  --tenant-rule: oklch(88% 0.01 52);
  --tenant-success: oklch(93% 0.07 150);
  --tenant-success-ink: oklch(38% 0.11 150);
  --tenant-warning: oklch(95% 0.08 85);
  --tenant-warning-ink: oklch(43% 0.11 68);
  --tenant-info: oklch(93% 0.05 250);
  --tenant-info-ink: oklch(40% 0.12 250);
}

.dark {
  --tenant-surface: oklch(18% 0.02 52);
  --tenant-surface-raised: oklch(22% 0.02 52);
  --tenant-ink: oklch(95% 0.01 90);
  --tenant-ink-muted: oklch(73% 0.02 90);
  --tenant-soft: oklch(28% 0.025 52);
  --tenant-soft-ink: oklch(95% 0.01 90);
  --tenant-action: oklch(76% 0.13 255);
  --tenant-on-action: oklch(18% 0.02 52);
  --tenant-danger: oklch(73% 0.18 28);
  --tenant-on-danger: oklch(18% 0.02 52);
  --tenant-rule: oklch(35% 0.02 52);
  --tenant-success: oklch(30% 0.08 150);
  --tenant-success-ink: oklch(86% 0.09 150);
  --tenant-warning: oklch(32% 0.08 85);
  --tenant-warning-ink: oklch(89% 0.09 85);
  --tenant-info: oklch(30% 0.06 250);
  --tenant-info-ink: oklch(87% 0.07 250);
}
```

Use a relative asset URL such as `theme/fonts/tenant-sans.woff2`, not an
absolute `/theme/...` URL. Relative resolution preserves non-root app mounts.
Never use `!important`: it defeats the low-specificity fallback contract.

## Delivery prerequisite

The current `app/default` manifest is a legacy `kind: app` package. Gestaltd's
native theme endpoint belongs to a `kind: ui` mount, so this package must not
be configured directly under `providers.ui` yet. The clean platform migration
is to ship this static bundle as a native UI artifact with an `assetRoot` and
route policy, then configure that artifact as the mounted UI. It is a packaging
and deployment migration, not a tenant-theme concern.

The target package has a native UI manifest separate from this legacy app
manifest:

```yaml
kind: ui
source: github.com/acme/home-ui
version: 1.0.0
install:
  command: [npm, ci]
  inputs: [package-lock.json]
build:
  command: [npm, run, build]
  inputs: [package.json, package-lock.json, src, shared, index.html]
dev:
  command: [npm, run, dev]
spec:
  assetRoot: dist
  routes:
    - path: /*
      allowedRoles: [viewer]
```

Choose the actual source, inputs, and route policy for the deployment. The
important boundary is `kind: ui` plus `spec.assetRoot: dist`; do not overload
the theme configuration onto a legacy app-static block.

Once that native UI artifact exists, the deployment owns the stylesheet and
assets directory. Its config has this shape:

```yaml
apiVersion: gestaltd.config/v8
providers:
  ui:
    home:
      path: /portal
      source:
        path: ./home-ui
      config:
        theme:
          stylesheet: ./themes/tenant/theme.css
          assetsDir: ./themes/tenant/assets
```

Gestaltd then serves the stylesheet at `/portal/theme.css` and assets below
`/portal/theme/`. The Vite build uses relative asset URLs and detects the
mount path for client routing, so the future UI artifact can run at either the
root or a non-root mount on canonical client routes such as `/portal/apps`.

The native UI host should also either inject `<base href="/portal/">` into the
returned index document or redirect trailing-slash deep links such as
`/portal/apps/` to their canonical form. That is a generic static-hosting
requirement: without it, browser-relative assets resolve below the deep route
instead of the mount. It belongs in Gestaltd's mount renderer, not in a
tenant theme or this bundle.

## Development

Run Vite against the local Gestaltd or cookie-proxy origin. Vite proxies
`/api`, `/theme.css`, and `/theme/*` to that same origin, so the native UI
package uses the identical endpoint shape as production. When Gestaltd starts
a native UI in dev mode, it provides `GESTALT_DEV_BASE_PATH`; the Vite config
uses that path for its module and HMR URLs. Direct `npm run dev` continues to
serve at the root.

```bash
GESTALT_API_PROXY_TARGET=http://127.0.0.1:8080 npm run dev
```

Configure the theme in the local deployment; do not point this public project
at a tenant stylesheet on disk. There is one theme delivery path, not a Vite
mirror or a second development-only asset server.

The current Gestaltd development handler needs a server follow-up to match the
production contract for an unconfigured stylesheet endpoint and non-root
mounted theme assets. This app intentionally does not emulate those cases in
Vite; use a server version with that parity fix when validating the native UI
package locally.

## Component synchronization

The companion private Registry may own tenant-specific theme values and
extensions, but public-safe components must be exported against this contract.
See [`UI_CORE_SYNC.md`](UI_CORE_SYNC.md) for the compatibility and export
boundary. The contract check runs in `npm run check` to prevent private token
namespaces and raw palette utilities from leaking back into this bundle.

## Follow-up boundary

The static admin shell under `public/admin/` has its own legacy stylesheet and
is not yet part of this runtime theme contract. It should be migrated as a
separate, visual-regression-tested change rather than coupled to this core
cutover.
