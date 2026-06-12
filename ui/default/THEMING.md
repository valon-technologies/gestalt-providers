# Theming

This UI ships with a **generic theme** and is re-skinned per tenant without
rebuilding the bundle. A theme is a set of CSS custom properties; the config
that selects and provides those properties belongs to the tenant's Gestalt
deployment repo, not to this one. Delivery is **serve-time**: the root layout
always links `<link rel="stylesheet" href="/theme.css">`, and gestaltd serves
the deployment-configured stylesheet there (an empty `200 text/css` when no
theme is configured, so the generic theme renders by default — no FOUC, no
JS).

## Theme contract

All themeable values flow through CSS custom properties declared in
[`shared/theme.css`](shared/theme.css) (`:root` for light, `.dark` for
dark).

| Token | Purpose | Generic default |
| --- | --- | --- |
| `--background` / `--surface` / `--surface-raised` | page + card surfaces | warm near-whites |
| `--border`, `--foreground`, `--alpha-dark` | lines, text, alpha text/border scale | warm neutrals |
| `--shadow-ink` | shadow color (RGB triplet) — identical in light and dark by default, so shadows never become glows | `35, 24, 16` |
| `--brand`, `--brand-soft`, `--danger`, `--success` | brand accent pair / status colors | gold pair / red / green |
| `--radius` | base corner radius; Tailwind derives `rounded-sm` (−2px), `rounded`/`rounded-md` (as is), `rounded-lg` (+4px) | `0.5rem` (→ 6/8/12px) |
| `--content-max-width` | shared max width of the nav and every contained page *(ships with the Container PR)* | `80rem` |
| `--heading-weight` | default `h1`–`h6` weight (applied by a `globals.css` base rule) | `400` (Newsreader ships one cut) |
| `--font-display`, `--font-body`, `--font-mono` | type stacks (see the font seam below) | Newsreader (opsz 72) / Instrument Sans / Geist Mono — all OFL |

The bundled defaults are wrapped in `:where(:root)` / `:where(.dark)` — zero
specificity — so any tenant declaration (`:root { … }`, `.dark { … }`)
outranks them by cascade contract, not by observed stylesheet order.

A tenant theme is one stylesheet that re-declares any subset of these tokens
(plus optional `@font-face` rules). Conventions and rules:

- Keep raw values (hex/HSL literals) on **brand palette constants** at the
  top of the theme file; system tokens map to palette names, never to raw
  values — the file reads palette first, mapping second.
- Color tokens overridden for light must also be overridden for dark
  (`.dark { … }`), or dark mode renders a half-themed mix.
- **Never use `!important`** — it inverts the `:where()` armor.
- Only `:root`, `.dark`, and `body` selectors (plus `@font-face`, `@media`,
  `@supports`) are allowed. A theme styles tokens, not components.

### Fonts (the `-default` seam)

`next/font` cannot set `--font-*` directly in a way a tenant can override:
it emits its variables via a hashed class on `<body>` (specificity 0,1,0),
which would silently beat any tenant declaration. So the bundled fonts
declare `--font-display-default` / `--font-body-default` /
`--font-mono-default` instead, and `globals.css` re-maps them at zero
specificity:

```css
:where(body) {
  --font-display: var(--font-display-default);
  /* …body, mono */
}
```

Everything in the app consumes `--font-display` / `--font-body` /
`--font-mono` (never the `-default` names). A tenant overrides fonts by
declaring the consumed names **on `body`** (a bare `:root` declaration is
beaten by the `body`-level seam):

```css
@font-face { font-family: TenantSans; src: url(/theme/TenantSans.woff2); }
body { --font-body: TenantSans; }
```

Font files travel as theme assets under `/theme/` (see below), referenced by
absolute URLs.

### Caveat: alpha modifiers on brand/status utilities

`brand`, `brand-soft`, `danger`, and `success` map to raw hex custom
properties, so Tailwind alpha modifiers (`text-danger/50`, `bg-brand/10`)
do **not** apply to them — only opaque uses. The HSL-triplet tokens
(`background`, `surface`, `foreground`, `border`) keep full
`<alpha-value>` support.

## Where tenant config lives

A Gestalt deployment mounts this bundle as a ui provider
(`providers.ui.<name>`), pinned to an immutable, content-addressed
snapshot. Because the artifact is immutable and environment-agnostic,
tenant theming is applied at serve time:

1. This app's root layout links `/theme.css` after its own styles; gestaltd
   intercepts that path on the mount and serves the configured stylesheet
   (empty `200` when unconfigured).
2. The ui mount in the deployment repo points its `config.theme` at the
   tenant stylesheet (paths resolve relative to the deployment config file;
   schema in [`schemas/config.schema.yaml`](schemas/config.schema.yaml)):

   ```yaml
   # tenant deployment repo: deploy/config.yaml
   providers:
     ui:
       root:
         source: { git: … }   # unchanged, pinned snapshot
         path: /
         config:
           theme:
             stylesheet: ./ui/theme.css
             assetsDir: ./ui/theme-assets   # optional; served under /theme/
   ```

3. `deploy/ui/theme.css` is the tenant theme — versioned, reviewed,
   and deployed exactly like the rest of the deployment's config overlays.
   Assets in `assetsDir` (typically font files) are served under `/theme/`,
   a path the artifact does not own (`/fonts/*` it does).

Alternatives considered and rejected (full record in RES-20260612-002):

- **Theme tokens over the runtime API** — runtime fetch + JS-applied
  variables; flashes the generic theme and turns a styling concern into API
  surface. Keep as fallback if a tenant ever needs *dynamic* theming.
- **Serve-time HTML templating** — generalizing gestaltd's admin-shell HTML
  rewriting to mounted UIs is more invasive than serving one stylesheet.
- **Build-time delivery in any form** (private npm/git dependency,
  submodule, per-tenant builds) — bakes private branding and licensed fonts
  into the public immutable artifact and breaks the single-snapshot model.

## Local development against a live tenant theme

> **Never run a dev server from automation or CI** — and never symlink into
> `.dev/`: a symlink whose target leaves the project root puts Turbopack in
> its out-of-root resolution failure mode, which has OOM-crashed a dev
> machine (ISS-20260612-003). The mirror scripts now `rm` the mirror before
> every copy so a leftover symlink cannot survive, and `turbopack.root` is
> pinned to this package — do **not** widen it to a common ancestor
> directory to "fix" cross-repo resolution; that puts multi-gigabyte
> sibling checkouts on the watch list.

The root layout imports `@theme.css`, which resolves to the empty
`src/app/theme.stub.css` by default. Two dev loops:

**Mirror loop (HMR, primary).** Point `GESTALT_THEME_FILE` at the stylesheet
(e.g. in `.env.local`, which is gitignored):

```bash
GESTALT_THEME_FILE=/path/to/deployment-repo/deploy/ui/theme.css
```

Run `npm run dev:theme` alongside the dev server. It copies the source into
`.dev/theme.css` (gitignored) whenever it changes, and the dev server
hot-applies the CSS. Without the watcher, `next dev` still picks up a
snapshot of the theme at startup. The mirror exists because Turbopack only
resolves and watches files inside the project root.

**Served loop (prod parity, secondary).** In dev, `/theme.css` is rewritten
to the local gestaltd (`GESTALT_API_URL`), so the layout's link exercises
the real serve path — configure the theme on the local deployment and
reload. No HMR, but bit-exact with production delivery.

Production is unaffected by any of this: `next build` always bundles the
empty stub, and tenant themes arrive at serve time via `/theme.css`.

## Known gaps (follow-ups before the generic theme is truly generic)

- The static Tailwind palettes (`base`, `gold`, `grove`, `ember`) are still
  hardcoded warm hexes used throughout components — they bypass the token
  seam and won't respond to a tenant stylesheet (ISS-20260610-008). Note the
  generic `--brand` (#7a4f10) is gold-800, not the gold-500 accent — the
  palette→token migration is a usage sweep, not a rename.
- The `.dark dialog::backdrop` scrim keeps its own constant (the dark
  `--alpha-dark` triplet is a text scale, not a scrim color); light
  backdrop, shadows, radii, and brand/status colors are token-driven now.
- ~~Licensed fonts in the bundle~~ — resolved: bundled faces are OFL only
  (Newsreader / Instrument Sans / Geist Mono, licenses in
  `public/fonts/OFL-*.txt`; same branding as gestalt/docs). Commercially
  licensed fonts may not live in this public repo — tenant themes deliver
  them via `@font-face` against `/theme/fonts/…` (`theme.assetsDir`).
- The admin shell (`src/admin-static-assets.ts` → `out/admin/`) embeds its
  own copy of `theme.css` and its own font pipeline; it must be re-themed
  separately.
