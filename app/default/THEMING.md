# Theming

`app/default` is a public, tenant-neutral bundle. It ships a complete generic
fallback and receives an optional tenant stylesheet at runtime; it never
imports a tenant palette, font, filesystem path, or build dependency.

The browser always requests the relative URL `theme.css`. At a root mount that
is `/theme.css`; at `/portal/` it is `/portal/theme.css`. This is a delivery
contract: the hosting server selects and serves that stylesheet from deployment
configuration. A conforming server reserves the endpoint even when a
deployment has no theme, returning an empty `text/css` response so the generic
fallback renders normally.

## Public contract

The contract is defined by [`shared/theme.css`](shared/theme.css). It is
deliberately shadcn-shaped:

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

Filled surfaces use their matching foreground role: `bg-card` with
`text-card-foreground`, `bg-primary` with `text-primary-foreground`, and so on.
Do not mix fill and foreground roles merely because the default colors happen
to contrast; deployments may override every pair independently.

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

## Delivery

`app/default` is a unified `kind: app` package. A deployment mounts its prepared
static assets and optionally supplies the theme stylesheet and asset directory:

```yaml
apps:
  home:
    source: ./path/to/app/default/manifest.yaml
    static:
      mount: /portal
      theme:
        stylesheet: ./themes/tenant/theme.css
        assetsDir: ./themes/tenant/assets
```

Theme paths are relative to the deployment configuration. Gestalt serves the
stylesheet at `{mount}/theme.css` and the asset directory below
`{mount}/theme/`. When no stylesheet is configured, the stylesheet endpoint
still returns an empty `text/css` response so the generic fallback remains
usable.

The static host injects the mount as the document base. Consequently, the
relative `theme.css` link and theme asset URLs such as
`theme/fonts/tenant-sans.woff2` work at both `/` and non-root mounts.

This repository owns its app-local components. A deployment owns its theme
values and assets. Their interoperability boundary is only the semantic custom
properties documented here.

## Follow-up boundary

The static admin shell under `public/admin/` has its own legacy stylesheet and
is not yet part of this runtime theme contract. It should be migrated as a
separate, visual-regression-tested change rather than coupled to this core
cutover.
