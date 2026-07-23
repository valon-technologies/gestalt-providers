# Theming

This UI ships with a **generic theme** and is re-skinned per tenant without
rebuilding the bundle. A theme is a set of CSS custom properties; the config
that selects and provides those properties belongs to the tenant's Gestalt
deployment repo, not to this one. Delivery is **serve-time**: the root layout
always links `<link rel="stylesheet" href="theme.css">`, and gestaltd serves
the deployment-configured stylesheet there (an empty `200 text/css` when no
theme is configured, so the generic theme renders by default — no FOUC, no
JS).

## Theme contract

All themeable values flow through CSS custom properties declared in
[`shared/theme.css`](shared/theme.css) (`:root` for light, `.dark` for
dark).

| Token | Purpose | Generic default |
| --- | --- | --- |
| `--background` / `--surface` / `--surface-raised` | page + card surfaces | white / warm near-white |
| `--border`, `--foreground`, `--alpha-dark` | lines, text, alpha text/border scale | warm neutrals |
| `--shadow-ink` | shadow color (RGB triplet) — identical in light and dark by default, so shadows never become glows | `35, 24, 16` |
| `--brand`, `--brand-soft`, `--danger`, `--success` | brand accent pair / status colors | Registry gold-800 / gold-200 / ember / grove |
| `--accent`, `--accent-subtle`, `--accent-vivid`, `--accent-wash`, `--accent-solid` | interaction **fills** (Registry gold ladder) | gold-200 / mix / gold-300 / gold-100 / gold-400 |
| `--accent-foreground`, `--accent-vivid-foreground` | **ink on accent fills** (always `--foreground`) | same as `--foreground` |
| `--ring` | keyboard focus outline | gold-300 (Registry) |
| `--radius` | base corner radius; the CSS theme (`@theme` in `src/globals.css`) derives `rounded-sm` (−2px), `rounded`/`rounded-md` (as is), `rounded-lg` (+4px) | `0.5rem` (→ 6/8/12px) |
| `--content-max-width` | shared max width of the nav and every contained page (see `src/components/Container.tsx`) | `80rem` |
| `--heading-weight` | default `h1`–`h6` weight (applied by a `globals.css` base rule) | `400` (Newsreader ships one cut) |
| `--font-display`, `--font-body`, `--font-mono` | type stacks (see the font seam below) | Newsreader (opsz 72) / Instrument Sans / Geist Mono — all OFL |
| `--text-2xs` (Tailwind `@theme`) | absolute type-scale floor → `text-2xs` utility | `0.625rem` / 10px (below Tailwind `text-xs`) |

The bundled defaults are wrapped in `:where(:root)` / `:where(.dark)` — zero
specificity — so any tenant declaration (`:root { … }`, `.dark { … }`)
outranks them by cascade contract, not by observed stylesheet order.

A tenant theme is one stylesheet that re-declares any subset of these tokens
(plus optional `@font-face` rules). Conventions and rules:

- Keep raw values (`oklch()` literals) on **brand palette constants** at
  the top of the theme file; system tokens map to palette names, never to
  raw values — the file reads palette first, mapping second.
- Color tokens overridden for light must also be overridden for dark
  (`.dark { … }`), or dark mode renders a half-themed mix.
- **Never use `!important`** — it inverts the `:where()` armor.
- Only `:root`, `.dark`, and `body` selectors (plus `@font-face`, `@media`,
  `@supports`) are allowed. A theme styles tokens, not components.

### Interaction roles (accent fill vs on-fill ink)

`--accent*` tokens are **roles**, not “make the text gold.” They exist so
ported UI kit chrome (Switch, nav, list selection) can keep Registry semantic
class names (`bg-accent-solid`, `bg-accent-subtle`, …) without inventing
mappings to `--brand`.

Defaults match Valon Registry (`valon-tools/registry/theme/theme.css`):

| Role | Means | Generic default |
| --- | --- | --- |
| `--accent` / `--accent-subtle` | soft selected / wash **fills** | gold-200 (+ mix with `--background` for subtle) |
| `--accent-vivid` | bright stroke / checked fill | gold-300 |
| `--accent-solid` | mid control fill (Switch on, link ink) | gold-400 |
| `--accent-wash` | pale tint | gold-100 |
| `--accent-foreground` / `--accent-vivid-foreground` | text **on** those fills | `--foreground` (ink) |
| `--brand` / `--brand-soft` | brand passthrough (AA text / soft wash) | gold-800 / gold-200 |

**Invariant:** accent roles color the fill; on-accent text stays ink. Never
use `text-brand` for a selected nav/list row. Tenant themes should override
the gold ladder (or at least `--brand`, `--brand-soft`, `--accent-solid`,
`--accent-vivid`) in both `:root` and `.dark` — see `shared/theme.css`.

When adapting shared UI kit components into `src/components/ui/`, see
[`src/components/ui/PORTING.md`](src/components/ui/PORTING.md).

### Fonts (the `-default` seam)

Bundled fonts declare `--font-display-default` / `--font-body-default` /
`--font-mono-default` at zero specificity in `globals.css`, and re-map them to
the consumed `--font-*` tokens on `:where(body)`. Everything in the app
consumes `--font-display` / `--font-body` / `--font-mono` (never the
`-default` names). A tenant overrides fonts by declaring the consumed names
**on `body`** (a bare `:root` declaration is beaten by the `body`-level seam):

```css
@font-face { font-family: TenantSans; src: url(/theme/TenantSans.woff2); }
body { --font-body: TenantSans; }
```

Font files travel as theme assets under `/theme/` (see below), referenced by
absolute URLs.

### Color format and alpha

Every color token is a whole `oklch()` color (no triplet formats). Alpha
modifiers on token-backed utilities (`text-danger/50`, `bg-brand/10`,
`ring-foreground/10`, …) compile to `color-mix()` under Tailwind v4 and
work on every color token. Ink roles follow the Registry contract
(`text-foreground`, `text-muted-foreground`, `text-primary-foreground` on
fills). Optional console-only `text-faint` / `border-alpha` / `bg-alpha-*`
are pre-mixed from `--alpha-dark` at set percentages — override the token,
not the percentages. Any valid CSS color works in a tenant theme;
`oklch()` is the house format (matches gestalt/docs and Tailwind v4's
own palette).

## Where tenant config lives

A Gestalt deployment mounts this bundle as an app static bundle
(`apps.<name>.static`), pinned to an immutable release. Because the artifact
is immutable and environment-agnostic, tenant theming is applied at serve time:

1. This app's root layout links `theme.css` after its own styles; gestaltd
   intercepts that path on the mount and serves the configured stylesheet
   (empty `200` when unconfigured).
2. The app mount in the deployment repo points its `theme` at the tenant
   stylesheet (paths resolve relative to the deployment config file):

   ```yaml
   # tenant deployment repo: deploy/config.yaml
   apps:
     home:
       source:
         git:
           repo: valon-technologies/gestalt-providers
           path: app/default/manifest.yaml
       static:
         mount: /
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

The root layout imports `@theme.css`, which resolves to the empty
`src/theme.stub.css` by default. Two dev loops:

**Mirror loop (HMR, primary).** Point `GESTALT_THEME_FILE` at the stylesheet
(e.g. in `.env.local`, which is gitignored):

```bash
GESTALT_THEME_FILE=/path/to/deployment-repo/deploy/ui/theme.css
# Optional override; defaults to the directory containing GESTALT_THEME_FILE
# (same shape as deploy `assetsDir: ./ui` + `stylesheet: ./ui/theme.css`).
# GESTALT_THEME_ASSETS_DIR=/path/to/deployment-repo/deploy/ui
```

For Valon / Registry face parity (Season Serif + Melange), Vite also auto-
discovers `~/Work/toolshed/valon-tools/deploy/ui/theme.css` when
`GESTALT_THEME_FILE` is unset in development. That theme owns `@font-face`
against `/theme/fonts/…` and sets `body { --font-display/--font-body/--font-mono }`
— do not fork those faces in the console bundle.

Vite then:

1. Mirrors the stylesheet into `.dev/theme.css` for the `@theme.css` import
   (and `npm run dev:theme` watches for HMR).
2. Serves the production theme contract on the Vite origin:
   `/theme.css` ← stylesheet, `/theme/*` ← assetsDir (brand `@font-face`
   files at `/theme/fonts/…`). Without (2), tenant font URLs fall through
   to the SPA HTML shell and the UI renders with system fallbacks.

**Served loop (prod parity, secondary).** With `gestaltd serve`, `theme.css`
is served by gestaltd on the mount, so the layout's link exercises the real
serve path — configure the theme on the local deployment and reload. No HMR,
but bit-exact with production delivery.

Production is unaffected by any of this: `npm run build` always bundles the
empty stub, and tenant themes arrive at serve time via `theme.css`.

## Known gaps (follow-ups before the generic theme is truly generic)

- The static Tailwind palettes (`base`, `gold`, `grove`, `ember` — now
  `--color-*` entries in the `@theme` block of `src/globals.css`) are
  still hardcoded warm hexes used throughout components — they bypass the
  token seam and won't respond to a tenant stylesheet (ISS-20260610-008).
  Note the generic `--brand` (gold-800) is for AA text on white; Switch /
  control fills use `--accent-solid` (gold-400) — same ladder as Registry.
- The `.dark dialog::backdrop` scrim keeps its own constant (the dark
  `--alpha-dark` triplet is a text scale, not a scrim color); light
  backdrop, shadows, radii, and brand/status colors are token-driven now.
- ~~Licensed fonts in the bundle~~ — resolved: bundled faces are OFL only
  (Newsreader / Instrument Sans / Geist Mono, licenses in
  `public/fonts/OFL-*.txt`; same branding as gestalt/docs). Commercially
  licensed fonts may not live in this public repo — tenant themes deliver
  them via `@font-face` against `/theme/fonts/…` (`theme.assetsDir`).
- The admin shell (`public/admin/`) embeds its own copy of `theme.css` and
  its own font pipeline; it must be re-themed separately.
