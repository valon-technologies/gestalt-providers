# Agent instructions — gestalt-providers

This is a **public, tenant-neutral** UI platform repository.

## Theming (read before editing CSS)

- Contract: [`app/default/THEMING.md`](app/default/THEMING.md)
- Runtime tokens: [`app/default/shared/theme.css`](app/default/shared/theme.css)
- Tailwind bridge: [`app/default/src/globals.css`](app/default/src/globals.css) (`@theme inline`)
- Agent guardrails: [`docs/agent/theme-boundary.md`](docs/agent/theme-boundary.md)
- Tenant theme coverage test: [`app/default/src/lib/tenant-theme.contract.test.ts`](app/default/src/lib/tenant-theme.contract.test.ts) (manifest: [`app/default/shared/tenant-theme-manifest.json`](app/default/shared/tenant-theme-manifest.json))

### Hard rules

1. **Never** add deployment-specific palette constants to this repo (CSS variables
   with an org-specific prefix such as `--tenant-*`).
2. **Never** add licensed brand fonts or tenant `@font-face` rules here.
3. **Never** import or reference a deployment repo's `deploy/ui/theme.css` from
   bundle source.
4. Components use **semantic** Tailwind utilities only (`text-display-sm`,
   `bg-card`, `text-muted-foreground`) — not raw palette names or hardcoded brand
   values.

### Where tenant branding goes

Tenant themes and platform identity are served at runtime from the **deployment
repo**:

```text
<deployment-repo>/deploy/ui/theme.css   → served as /theme.css
<deployment-repo>/deploy/ui/mark.svg    → served as /theme/mark.svg (optional)
<deployment-repo>/deploy/config.yaml    → apps.<name>.static.theme + static.brand
```

- **Theme** (`static.theme`): colors, fonts, radii — CSS custom properties.
- **Brand** (`static.brand`): product name + optional mark — `/brand.json`.
  Chrome reads it via `usePlatformBrand()` (default: Gestalt). Never hardcode
  a tenant product name in this repo.

Tenants declare palette constants at the top of their theme file, then map them
onto the semantic names this bundle consumes (`--foreground`, `--text-display-sm`,
…). Local dev: `GESTALT_THEME_FILE=/path/to/deploy/ui/theme.css` (see THEMING.md);
optional `GESTALT_BRAND_NAME` / `deploy/ui/brand.json` for the top-bar name.

### Porting Registry components

When a vendored primitive needs a token that is missing:

1. Add a **generic** name + neutral default to `shared/theme.css`.
2. Bridge it in `globals.css` `@theme inline`.
3. Put tenant-specific values in the deployment repo's `deploy/ui/theme.css`.

Do **not** copy palette constants from a private Registry theme into this public
bundle.

## PR review

For pull requests, follow [`docs/agent/theme-boundary.md`](docs/agent/theme-boundary.md)
and run the `/pr-review` skill when available.
