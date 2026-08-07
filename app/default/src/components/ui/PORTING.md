# Porting shared UI kit components

Registry primitives live in `src/components/ui/` (kebab-case filenames matching
Registry: `badge.tsx`, `link.tsx`, `radio-group.tsx`, `tooltip.tsx`, …).
Composition recipes that are not standalone components (e.g. choice-card chrome)
live in `src/lib/`. Console-specific composed UI (`Nav`, `IntegrationCard`,
`TokenCreateForm`) stays in `src/components/`.

When lifting a shared UI kit control into `src/components/ui/`:

1. **Keep semantic class names** (`bg-accent-subtle`, `text-accent-foreground`,
   `bg-accent-vivid`, `bg-accent-solid`, …). Do not reinterpret “accent” as
   “brand-colored text.”
2. **Map only through the theme bridge** in `shared/theme.css` /
   `globals.css`. Those aliases already mean (Registry-aligned):
   - accent / accent-subtle → soft gold-200 washes
   - accent-vivid → brand-soft selected fill/stroke
   - accent-solid → brand control fill (Switch checked)
   - accent\*-foreground → ink (`--foreground`)
3. **Body / secondary copy:** use `text-muted-foreground` (and
   `text-muted-foreground-soft` for tertiary). Never `text-muted` or
   `text-secondary` for ink — Tailwind v4 maps those names to surface tokens.
4. **Forbidden on selected chrome:** `data-active:text-brand`,
   `data-[selected]:text-brand`, `data-active:text-gold-*`, and the same for
   `data-[state=active]`. Selected rows use ink on an accent fill.
5. Adapt motion / focus / sizing to local tokens (`focus-ring`,
   `duration-select-*`, control heights) — not color roles.

`oxlint` enforces (4) via `home/no-brand-text-on-selected`
(`oxlint-plugin-home.mjs`, scoped to `src/components/ui/**`).

## Button / Input / Field / Label / Select

Registry `button`, `input`, `field`, `label`, `select`, `spinner`, and `brand-spinner` are
vendored here. Theme bridges (`--primary`, `--muted`, `--input`, `--disabled*`,
`--state-overlay-*`, `--control-*`) live in `shared/theme.css` + `globals.css`
`@theme inline`. Spinner motion CSS (`.valon-spinner-trail`, BrandSpinner mark
keyframes) lives in `globals.css` and maps BrandSpinner strokes to semantic
tokens (`--border`, `--accent-strong`) — not Registry palette constants.
Prefer `@/components/ui/button`, `@/components/ui/input`, and
`@/components/ui/select` at call sites;
`@/components/Button` is a legacy adapter (`primary` → `default`,
`danger` → `destructive`).

`Button` `loading` shows the routine trail `Spinner`, sets `aria-busy` /
`aria-disabled`, and keeps enabled chrome — never conflate transient busy with
`disabled`. Use `BrandSpinner` only for rare brand/identity waits.

**One color contract (Registry):** ink is `text-foreground` /
`text-muted-foreground`; fills are `bg-primary` / `bg-muted` / `bg-secondary`.
Do not reintroduce console `text-primary` / `text-muted` / `text-secondary`
ink utilities — those names are Registry fills. Optional console-only
`text-faint` remains for tertiary chrome (40% alpha-dark).

Compose labeled controls with `Field` + `FieldLabel` (+ `FieldDescription` /
`FieldError`) — see Registry `guidelines/fields.md`. Use Registry `Select`
(not native `<select>`) for filter / form dropdowns.

`Input` / `Textarea` expose a `chrome` variant (`standalone` default,
`group` for `InputGroupInput` / `InputGroupTextarea`) so the shell owns the
focus ring without stacking on the inner control.

## Avatar

Registry `avatar` is vendored here (`Avatar`, `AvatarImage`, `AvatarFallback`,
`avatarVariants`). Solid fill is `bg-muted-strong` (not `bg-muted`) so the disc
stays distinct on Neutral / muted row hover — bridge `--muted-strong` in
`shared/theme.css` + `globals.css`. Strip `"use client"`. Prefer
`@/lib/cn` over Registry `@/lib/utils`.

## Badge / HoverCard

Registry `badge` and `hover-card` are vendored here (toolshed#4057 / #4081).
Badge `size` owns type / pad / icon; base has **no** color transitions. Ghost
shares `@/lib/press-feedback` quiet chrome with Button (never `hover:bg-accent`);
muted hover climbs `--neutral-dark-hover`. Keep status variants on `--badge-*`
(`bg-badge-success`, …), not Registry `bg-success` / `error`. Strip `"use client"`;
import `@/lib/cn`. HoverCard JSDoc documents controlled-open + trigger remount
(Registry `guidelines/flyout.md`).

## Choice cards (RadioGroup + Switch)

Do not fork tile chrome at call sites. Import helpers from
`@/lib/choice-card-chrome`. Primitives come from `@/components/ui/radio-group`:

- `choiceCardClassName` + `choiceCardHoverClassName` — simple tiles
- `choiceCardFormShellClassName` + `choiceCardFormFieldsClassName` — nested fields
  (`Collapsible` drawer outside the `Label`, per `nested-interactive.md`)
- `choiceCardRadioClassName` / `choiceCardRadioEyebrowClassName` — radio placement
- Pass `focusRing="none"` on `RadioGroupItem` inside choice cards

Canonical: upstream `choice-card-chrome` + `radio-group` stories. Requires `--accent-solid` in theme.

**Switch Choice Card** (preference toggle): `FieldLabel` wraps `Field` +
`FieldTitle` + `Switch` — card chrome and neutral checked wash live on
`FieldLabel`; content-first horizontal `Field` places the Switch top-right.
See Registry Forms/Switch → ChoiceCard / `guidelines/fields.md`. Not `Alert`.

## Code (inline)

Registry `code` is vendored as `ui/code.tsx` (`Code` / `codeVariants`). Use for
inline identifiers / paths / flags in UI copy — not `CodeBlock`, not `Kbd`.
Do not hand-roll `bg-muted font-mono` at call sites.

## AppTopBar / AppLogo

Registry `app-top-bar` + `app-logo` are vendored here. Console `Nav` composes
`AppTopBar` slots (Start / Center / End) with `AppTopBarBrand` for the product
wordmark (`font-display` via `AppLogoName`). Do not hand-roll header chrome or
put `font-heading` / `font-bold` on the wordmark.

`AppLogoName` size scale is chrome-local (`default` / `md` / `lg` → heading-sm /
heading-lg / heading-xl). It is not PageHeader's display tiers — do not put
`text-display-*` back on the wordmark. Console Nav uses `size="lg"`.

**Console adaptations (keep when syncing):**

- Sticky stacking stays on `__root.tsx` (`DevWorktreeBanner` + chrome) — omit
  Registry `sticky top-0 z-50` on `AppTopBar`.
- Column clamp matches `Container` (`max-w-7xl px-6`), not Registry
  `px-4 md:px-6`, so the bar and pages stay aligned.

## Brand type scale

Registry `header-chrome`, `PageHeader`, and `SectionHeader` are vendored here.
`PageHeader` / `SectionHeader` are thin wrappers over `createHeaderChrome` — sync
all three together from the upstream Registry. Display tiers use
`font-display`; compact tiers use `font-sans`. Do not reintroduce a `display`
prop or face overrides at call sites.

Registry PageHeader / SectionHeader consume `text-heading-*`, `text-display-*`,
`tracking-heading`, `tracking-display` (brand type scale). Add **generic** defaults
in `shared/theme.css` and bridge them in `globals.css` `@theme inline`. Tenant-
specific values belong in the tenant deployment stylesheet — not here.
See [`docs/agent/theme-boundary.md`](../../../../docs/agent/theme-boundary.md).
Do not invent freestyle `tracking-*` / `text-*` sizes at call sites.

## RunStatusIndicator

Registry `run-status-indicator` is vendored here. Prefer it for workflow / job
outcome glyphs (succeeded / failed / running / pending / …). Map Registry
`bg-green-500` / `bg-red-500` / `bg-yellow-500` onto `--status-indicator-*`
(+ `text-white`) — same mid-chroma light-on-fill recipe as fleet replica dots.
Keep `runStatusIndicatorBadgeVariant` on this Badge API (`"destructive"`, not
Registry `"error"`). Import `@/lib/cn` (not Registry `@/lib/utils`).

## Card / Collapsible / Item

Registry `card`, `collapsible`, and `item` are vendored here. Application record
lists (workflow runs, activity, directories) compose `SectionHeader` **above** a
`Card` that wraps only `ItemGroup` rows — see Registry `application-lists.md`.
Do not hand-roll `ul.divide-y` when Item fits. Expand/collapse is owned by
`Collapsible` — paint the root with `cardVariants({ variant: "outline" })` at
the call site (cards.md Card Collapsible). Do not restyle trigger hover/press
(List Item Neutral via `listItemInteraction`). Drawer height animation lives on
`[data-slot=collapsible-content]` in `globals.css` (Registry theme keyframes).

## Tabs

Registry `tabs` is vendored as `ui/tabs.tsx` (line underline + sliding
indicator). Use for content navigation; mode switching stays on
`SegmentedControl`. Do not invent gold/brand underline chrome at call sites.

## SegmentedControl / ThemeToggle

Registry `segmented-control` ships `variant="default"` (borderless muted well on
paper) and `variant="outline"` (bordered well on muted chrome). ThemeToggle
forwards `variant`; keep local `placement` (`header` | `menu`). Use `outline`
only when the parent is muted (sidebar / rail).

## CodeBlock / code-fence

Registry `code-block` + `code-fence` are vendored here for display snippets
(Build MCP install, etc.). Keep highlighting on lowlight → `.typeset-code-hljs`
(`src/styles/typeset-code-hljs.css`). Do not reintroduce
Shiki for these surfaces. Shell paint maps Registry `bg-muted/50` /
`border-border/50` to console `bg-alpha-5` / `border-alpha`. Pass
`chrome="inset"` for docs/blog fences (no header; copy overlays the body).
Pass `filename` only for real file paths with `chrome="header"` (default) —
language is highlighting only, not a status label. Multi-file /
language-tab recipes (`MultiFileCodeBlock`, `LanguageTabsCodeBlock`) use
vendored `tabs`.

## Stepper

Registry `stepper` is vendored as `ui/stepper.tsx` (process navigation with
checks + connectors). Depends on `lib/list-item-interaction.ts` and
`selection-check`. Theme bridges include `--accent-fill-hover` /
`--accent-fill-pressed` for soft-selected hover (selectable-rows). Build page
uses controlled `activationMode="jump"` — do not restyle Stepper chrome at the
call site (layout-only wrappers OK).

## Pagination

Registry `pagination` is vendored as `ui/pagination.tsx`. `PaginationLink`
supports `asChild` for router links. Ellipsis sizing uses `size-control-default`
(local control tokens). Prefer Previous/Next (+ optional “N of M” counter) for
detail sibling navigation; do not invent custom pager chrome at call sites.

## PageLayout / NavList

Registry `page-layout`, `page-layout-pane-mobile-nav`, and `nav-list` are
vendored here. `PageLayout` owns in-page geometry (header band, start Pane,
content, end Aside); `PageLayoutPaneMobileNav` is secondary mobile chrome for
long-list `paneMobile` (sticky Menu bar under AppTopBar → overlay disclosure +
document scroll lock, Next.js docs pattern); `NavList` is router-agnostic
section navigation for rails, sheets, and flyouts. Set
`--page-layout-pane-top` / `--page-layout-pane-bottom` /
`--page-layout-mobile-nav-height` / `--page-layout-anchor-offset` and track widths
(`--page-layout-pane-width` / `--page-layout-aside-width`) in `globals.css` next
to nav-height tokens — do not scatter `sticky top-*` or hand-roll grid tracks per
page. Console sticky top is `calc(--app-sticky-chrome-height + --page-layout-pane-gap)`
so rails clear the measured banner+topbar stack with a constant gap. Use
`scroll-mt-[var(--page-layout-anchor-offset)]` on in-page anchors so
`scrollIntoView` clears sticky chrome + the mobile Menu bar (includes
`--page-layout-anchor-gap` breathing room). Use `tracks="compact"` (11rem for
both rails) for dense section rails; Settings keeps the default 13.75rem for
both. `NavListItem` defaults to outward `focus-ring`;
`PageLayoutPane` / `PageLayoutAside` own scrollport padding so rings stay visible.
Action rows use `actions` plus
`nestedInteractiveSuppress.selectableRowSiblingControl` from `@/lib/nested-interactive`.
For a handful of mobile destinations prefer `SegmentedControl` in `paneMobile`;
for longer lists prefer `PageLayoutPaneMobileNav`. Compose breadcrumbs in the
content column below the Menu bar and above the page title. Sticky for the Menu
bar is owned by `PageLayout`'s tall `page-layout-main` / `pane-mobile` wrapper
(same sticky model as AppTopBar, so overscroll bounce matches) — do not
`position: fixed` the closed bar. Host pages with Menu must not put top padding
on `Container` above the bar. Keep the Menu label column as
`mx-auto max-w-7xl px-6` (same as `AppTopBarInner`) so the caret aligns with the
brand. Do not put `overflow-x-*` on `PageLayout`'s `paneMobile` slot. Wrap a
scrolling `SegmentedControl` at the call site instead.

## SearchHighlight

Registry `search-highlight` + `search-highlight-context` are vendored as
`ui/search-highlight.tsx` and `lib/search-highlight-context.tsx` (helpers in
`lib/search-highlight.ts`). Highlight paint is decoupled from DataTable: pass an
explicit `query` prop or wrap with `SearchHighlightProvider`. Use
`variant="vivid"` on card surfaces (integrations catalog); default
`bg-accent-highlight` is for table cells when DataTable bridges context.
Do not hand-roll `<mark className="bg-accent-vivid">` at call sites.
Catalog filtering (`lib/integrationSearch.ts`, `lib/catalogFilters.ts`) must
delegate matching to `search-highlight.ts` — one normalization stack for filter
and highlight. Strip `"use client"` from vendored copies (Vite SPA; Registry
carry-over is a no-op here).

## Held local overrides (discuss before dropping)

### Add to Registry (so console can drop the fork)

| Item | Why |
| --- | --- |
| **TableOfContents `kind: "separator"`** | Apps catalog TOC divider between groups |
| **`AGENT_CONSOLE_THEME_CODEX` / `_CURSOR` exports** (optional) | Story palettes only today; Build re-copies them — promote from `agent-console.stories` |

### Avatar (synced)

Vendored with Registry: `sm`/`default`/`lg`/`xl`, `solid` → `bg-muted-strong`,
baseline-safe fallback (`block` + size-matched `leading-*`). Bundle default for
`--muted-strong` lives in `shared/theme.css` (tenant-neutral achromatic step);
bridge in `globals.css` `@theme inline`. Interactive account chips still pin
`bg-neutral-hover` and climb Neutral-dark — see AccountMenu / Registry AppTopBar
account-chip recipe.

### Keep as console adapters (not Registry gaps)

| Item | Why |
| --- | --- |
| **`theme-toggle.tsx`** | Thin wrapper on `@/hooks/use-theme` — Registry ships its own `useTheme`; app theme ownership stays local |
