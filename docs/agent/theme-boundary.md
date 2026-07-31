# Theme boundary — public bundle vs tenant deployment

Use this when adding or changing theme tokens, typography, colors, or fonts in
`gestalt-providers`.

## Problem this solves

Agents often copy a complete theme from a private Registry or deployment repo
into this public bundle. That bakes tenant branding into an immutable artifact
and breaks serve-time theming.

## Architecture

```text
shared/theme.css (:where defaults, tenant-neutral names)
  → tenant deploy/ui/theme.css (optional serve-time override at /theme.css)
  → globals.css @theme inline (Tailwind utilities → semantic CSS variables)
  → components (semantic class names only)
```

## What belongs where

| Layer | Location | Allowed |
| --- | --- | --- |
| Generic defaults | `app/default/shared/theme.css` | Semantic names: `--foreground`, `--text-display-sm`, `--tracking-display` |
| Tailwind bridge | `app/default/src/globals.css` | `--text-display-sm: var(--text-display-sm)` — references semantic names only |
| Tenant palette + mapping | `<deployment-repo>/deploy/ui/theme.css` | Org palette constants → semantic token overrides |
| Delivery | `<deployment-repo>/deploy/config.yaml` | `static.theme.stylesheet: ./ui/theme.css` |

## Deny list (public bundle)

Flag as **high severity** if added to `shared/theme.css`, `globals.css`, or
component source:

1. **Deployment palette prefixes** — CSS custom properties like `--tenant-*` or
   any org-specific palette namespace.
2. **`var(--tenant-*)` bridges** in `globals.css` `@theme inline`.
3. **`@font-face`** for licensed / commercial brand fonts.
4. **Imports or path references** to `deploy/ui/theme.css` or private Registry
   theme files.
5. **Hardcoded brand literals** in components (oklch/hex) except documented APIs.

## Allow list (public bundle)

- Semantic token defaults in `shared/theme.css` (`:where(:root)` / `:where(.dark)`).
- Identity bridges in `@theme inline` that reference those semantic names.
- OFL bundled fonts in `public/fonts/`.
- `THEMING.md` examples that show tenant mapping **inside a tenant stylesheet
  example block** (not in bundle source).

## Type scale example

**Public** (`shared/theme.css`):

```css
:where(:root) {
  --text-display-sm: 2.75rem;
  --text-display-sm--line-height: 3.125rem;
  --tracking-display: -0.03125rem;
}
```

**Public** (`globals.css` `@theme inline`):

```css
--text-display-sm: var(--text-display-sm);
--text-display-sm--line-height: var(--text-display-sm--line-height);
--tracking-display: var(--tracking-display);
```

**Tenant** (`deploy/ui/theme.css` — private deployment repo, not this repo):

```css
:root {
  /* Palette first (org-specific names OK here) */
  --tenant-text-display-sm: 2.75rem;

  /* Map onto consumed semantic names */
  --text-display-sm: var(--tenant-text-display-sm);
}
```

Tenants may also set semantic names directly (`--text-display-sm: 2.75rem`) without
an intermediate palette constant.

## Porting workflow

When `PageHeader` or another Registry primitive needs `text-display-sm`:

1. Confirm the utility is bridged in `globals.css`.
2. If the semantic CSS variable is missing, add a **generic default** to
   `shared/theme.css`.
3. Open a **separate change** in the deployment repo to override values in
   `deploy/ui/theme.css`.
4. Verify locally with `GESTALT_THEME_FILE` pointing at the deployment stylesheet.
5. Run the tenant-theme contract test (agents: grep `tenant-theme.contract`):

```bash
cd app/default
GESTALT_THEME_FILE=/path/to/deploy/ui/theme.css \
  bun test src/lib/tenant-theme.contract.test.ts
```

Manifest of required explicit overrides:
[`app/default/shared/tenant-theme-manifest.json`](../app/default/shared/tenant-theme-manifest.json).

## Detection (manual / review)

```bash
BASE=$(git merge-base main HEAD)

# Deployment palette prefixes in public theme files
git diff "$BASE"...HEAD -- app/default/shared/theme.css app/default/src/globals.css \
  | rg '--[a-z]+-text-|--tenant-|var\(--[a-z]+-'

# Legacy gestalt-shell tokens in components
git diff "$BASE"...HEAD -- '*.tsx' | rg 'border-alpha|bg-base-|text-faint|bg-surface'
```

## Remediation

| Finding | Fix |
| --- | --- |
| Org palette constant in `shared/theme.css` | Move to `deploy/ui/theme.css`; keep semantic name + generic default here |
| `var(--tenant-*)` in `@theme` bridge | Bridge to semantic name (`var(--text-display-sm)`) |
| Missing utility size | Add semantic default + `@theme` bridge in this repo; tenant values in deployment repo |
| Copied Registry theme block | Split: generic contract here, palette + overrides in deployment repo |
