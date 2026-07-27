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

## Alert

Registry `alert` is vendored here (`Alert`, `Callout`, `AlertIcon`, and the
collapsible parts). Status washes stay borderless; `variant="outline"` is quiet
Card chrome for CLI tips (no Tip primitive). Default and banner keep Title and
Description in one copy column beside a leading `AlertIcon` or `>svg`.
Description stacks under Title with `mt-1.5`. Description stays `text-foreground`
on default. `layout` is geometry only (default stacked grid vs wrapping banner).
Live region follows layout + variant, same as Registry: default (except outline)
is `role="alert"`; banner, chrome, and outline are not. Persistent in-page
guidance is `Callout` from this same module — stacked default grid, no live
region. Do not add a consumer `live` prop, and do not pick `layout="banner"` to
silence an Alert. Alert `layout="banner"` is an in-page wrapping toolbar; do
not fake full-bleed shell chrome with `rounded-none` / `border-b` — that is
Banner. Alert banner uses a container query so actions wrap under the copy
when the bar is narrow. Collapsible secondary help: `collapsible` +
`AlertTrigger` + `AlertCollapsibleContent` (+ optional `animateSize`). Drawer CSS
lives in `globals.css`. Button `secondary` on washes uses ink-alpha
(`secondarySurfaceFillClassName`), not solid `bg-secondary`.

## Banner

Registry `banner` is vendored here (`Banner` / `BannerIcon` / `BannerTitle` /
`BannerDescription` / `BannerActions` / `BannerClose`). Full-bleed
page/app-level system-message chrome: square, no border, no default live role.
`BannerTitle` is the optional kind label (worktree name, impersonation
target); keep it a sibling of `BannerDescription` so the root gap is the space
between them. Title and Description wrap unbroken strings (`wrap-break-word`);
do not truncate. Description mutes when Title is present. Geometry is owned —
do not pass `rounded-none` or `border-b` at the call site. Sticky placement
stays on `__root.tsx`. Prefer `@/lib/cn`. Dev worktree chrome composes Banner,
not Alert.

## Button / Input / Field / Label / Select

Registry `button`, `input`, `field`, `label`, `select`, `spinner`, and `brand-spinner` are
vendored here. Theme bridges (`--primary`, `--muted`, `--input`, `--disabled*`,
`--state-overlay-*`, `--control-*`) live in `shared/theme.css` + `globals.css`
`@theme inline`. Spinner motion CSS (`.spinner-trail`, `.brand-spinner*`)
lives in `globals.css` and maps BrandSpinner strokes to semantic tokens
(`--border`, `--accent-strong`) — not Registry palette constants. Class /
keyframe IDs match Registry.
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

Registry `badge` and `hover-card` are vendored here. Badge `size` owns type / pad / icon (`py-1` / `py-1.5` / `py-2`
after text-box trim); base has **no** color transitions. Ink trim (`text-box:
trim-both cap alphabetic`) lives on a `badge-label` text box via
`partitionBadgeChildren` — not on the `inline-flex` chrome (css-inline-3).
Ghost shares `@/lib/press-feedback` quiet chrome with Button (never
`hover:bg-accent`); muted hover climbs `--neutral-dark-hover`. Keep status
variants on `--badge-*` (`bg-badge-success`, …), not Registry `bg-success` /
`error`. Strip `"use client"`; import `@/lib/cn`. HoverCard JSDoc documents
controlled-open + trigger remount (Registry `guidelines/flyout.md`).

## MemberAccess

Registry `member-access` is vendored here (`MemberAccess`, `MemberAccessInvite`,
`MEMBER_ACCESS_REMOVE_VALUE`). Strip `"use client"`. Prefer `@/lib/cn` over
Registry `@/lib/utils`. Depends on local `alert-dialog`, `avatar`, `button`,
`item`, `people-picker`, `select`, `separator`. Pass optional `invite` (PeoplePicker
directory chrome + role); omit for service-account roster-only lists. Use
`disabled` for read-only grant previews until self-serve writes ship.

## PeoplePicker

Registry `people-picker` is vendored here. Strip `"use client"`. Prefer
`@/lib/cn`. Depends on local `avatar`, `button`, `command`, `popover`,
`selection-check`, and `@/lib/disclosure-caret`. Apps own `searchPeople`.

## Command / Popover

Registry `command` (`cmdk`) and `popover` are vendored for PeoplePicker /
Combobox flyouts. Strip `"use client"`. Prefer `@/lib/cn`. Popover collision
padding uses `FLYOUT_VIEWPORT_EDGE_INSET_PX` from `@/lib/flyout`.

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

Thin **workflow vocabulary adapter** over `OutcomeStatusIndicator`
(`succeeded` → `success`, `running` → `in_progress`, …) — Registry.
Root keeps run vocabulary on `data-status` (overrides outcome `data-status`).
Prefer `OutcomeStatusIndicator` for connection / deploy / non-run domains.

## OutcomeStatusIndicator

Registry `outcome-status-indicator` is vendored here —
domain-neutral filled circle + symbol. Map Registry mid-dark ramps onto
`bg-status-indicator-*` (`info` → `bg-status-indicator-info`). Failure Badge
pairing stays `destructive` (Registry `error` not vendored on Badge yet).
Distinct from uptime `StatusIndicator` dots. Table-row gutters use this
primitive (`iconOnly`); `TableStatusIndicator` is the `variant` adapter.

## TableStatusIndicator

Registry `table-status-indicator` is vendored here as a thin `variant` adapter
over `OutcomeStatusIndicator` (`danger` → `failure`, `info` → `info`,
`default` → `unknown`). Roots keep `data-slot` + `data-variant` and strip
`data-status`. Compact `iconOnly` gutters default to `sm`. Do not reintroduce
a second paint path (soft Badge washes) or a consumer `data-testid`.

## StatusIndicator

Registry `status-indicator` (Feedback/StatusIndicator) is vendored here —
dot + optional label for live/uptime style status. Map
Registry `--valon-*` fills onto `bg-status-indicator-*` (including idle →
`bg-status-indicator-muted`). Honor `pulse` (default `true`). Keep local
`data-slot` / `data-state` / `data-pulse` for tests and CSS hooks. No call
site yet — kept for Registry parity.

## Tree

Registry `tree` indent guides: keep `isolate` so `before:-z-10` does not paint
behind ancestor `bg-card`. Guide color is soft `--border` (not `--input`).
Guide inset is half the toggle gutter (`--tree-guide-inset`) so the line
centers on +/- — not `indent/2`. Keep the local `--tree-padding` formula
(`level * indent`) — Registry still uses `level - 1`.

## Card / Collapsible / Item

Registry `card`, `collapsible`, and `item` are vendored here. Application record
lists (workflow runs, activity, directories) compose `SectionHeader` **above** a
`Card` that wraps only `ItemGroup` rows — see Registry `application-lists.md`.
Do not hand-roll `ul.divide-y` when Item fits. Expand/collapse is owned by
`Collapsible` — paint the root with `cardVariants({ variant: "outline" })` at
the call site (cards.md Card Collapsible). Do not restyle trigger hover/press
(List Item Neutral via `listItemInteraction`). Drawer height animation lives on
`[data-slot=collapsible-content]` / `[data-slot=accordion-content]` in
`globals.css`, keyed by role-named keyframes (`accordion-drawer-*`,
`collapsible-maxwidth-*` for Alert `animateSize`).

## DescriptionList

Registry `description-list` is vendored here. Prefer over hand-rolled KV tables for
read-only metadata. Use `surface="outline"` for standalone inspector / docs
panels (Card outline fill/border, `rounded-lg` — not Card's `rounded-xl`).
Default `surface="plain"` stays flush in a parent pane. Row vertical rhythm is
`density="default"` (roomy) or `density="condensed"` (prior tight inspector) —
owned on the list, not per-item `py-*`. Terms use
`font-display text-sm italic tracking-wide text-muted-foreground`. Status value
tones use `--*-ink` canvas status tokens (bridged in `shared/theme.css` +
`globals.css`).

## StepPager

Registry `step-pager` is vendored here. Previous/next
destination cards for docs journey edges and Setup wizard steps. Compose
`StepPager` + `StepPagerPrevious` / `StepPagerNext` (+ `StepPagerStartSpacer`
when there is no previous). Use `asChild` for `<button>` or router `Link`.
Surfaces: `solid` (default) / `outline` / `ghost`. Setup uses `variant="ghost"`.
Not `Pagination` (dataset paging) and not `Stepper` (in-flow process rail).
Strip `"use client"`; prefer `@/lib/cn`. Hairline is semantic `border-border`.
Destination cards stay content-width (`w-fit max-w-full self-start`); the
title track is `max-content`, not `1fr`, so the Next caret sits beside the
label. Do not cap cards at `max-w-xs`: that forces an early wrap and the
box does not shrink back to the wrapped lines. The pager row uses
`items-start` so a taller neighbor cannot stretch the other card.

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
(Build MCP install, etc.). Keep highlighting on the curated
`code-block-lowlight` instance → `.typeset-code-hljs`
(`src/styles/typeset-reading.css`). Do not import `lowlight/all`, and do not
reintroduce Shiki for these surfaces. Shell paint maps Registry `bg-muted/50` /
`border-border/50` to console `bg-alpha-5` / `border-alpha`. Pass
`chrome="inset"` for docs/blog fences (no header; copy overlays the body).
Pass `filename` only for real file paths with `chrome="header"` (default) —
language is highlighting only, not a status label. Multi-file /
language-tab recipes (`MultiFileCodeBlock`, `LanguageTabsCodeBlock`) use
vendored `tabs`.

## Stepper

Registry `stepper` is vendored as `ui/stepper.tsx` (process navigation with
checks + connectors). Shared rail chrome lives in `ui/step-rail.tsx`. Depends on
`lib/list-item-interaction.ts` and `selection-check`. Theme bridges include
`--accent-fill-hover` / `--accent-fill-pressed` for soft-selected hover
(selectable-rows). Registry `completedChrome="outcome"` uses `--color-green-500`;
map that onto live `--status-indicator-success` (same OSI recipe). Do not copy
the Registry green ramp, and do not `var(--color-green-500)` here (`@theme inline
reference` does not emit that name). Setup uses a controlled horizontal Stepper
with `activationMode="linear"` and `size="default"`. Unreachable upcoming
steps are `disabled` (same rule as step navigation), so they do not take
hover, press, or click. Completed checks default to
`completedChrome="outcome"` (green fill, white check, ink connectors).
Pass `completedChrome="accent"` for gold. TimelineSteps defaults to gold
(`completedChrome="accent"`). Pass `completedChrome="outcome"` for green fill
and a white check (Setup token success). Do not restyle Stepper chrome at the
call site (layout-only wrappers OK). Step hover wash stays on the trigger;
the rail is `z-10` and the indicator is `z-20` so the connector stays visible
through the plate. Keep `isolate` on `StepperItem` (no z-index on the trigger).
Rail fill stagger and indicator chrome
delay honor `prefers-reduced-motion` (`motion-reduce:transition-none
motion-reduce:delay-0`; `readStepRailTimingMs` returns 0). Keep that mapping
on revendors.

## Pagination

Registry `pagination` is vendored as `ui/pagination.tsx`. `PaginationLink`
supports `asChild` for router links. Ellipsis sizing uses `size-control-default`
(local control tokens). Prefer Previous/Next (+ optional “N of M” counter) for
detail sibling navigation; do not invent custom pager chrome at call sites.

## PageLayout / NavList

Registry `page-layout`, `page-layout-pane-mobile-nav`, and `nav-list` are
vendored here (Menu vs rail sticky seams). `PageLayout` owns in-page
geometry (header band, start Pane, content, end Aside);
`PageLayoutPaneMobileNav` is secondary mobile chrome for
long-list `paneMobile` (sticky Menu bar under AppTopBar → modal dialog overlay
with FocusScope trap + `react-remove-scroll`, Next.js docs pattern); `NavList`
is router-agnostic section navigation for rails, sheets, and flyouts. Set sticky
offsets in `globals.css` next to nav-height tokens — do not scatter `sticky top-*`
or hand-roll grid tracks per page:
- `--page-layout-mobile-nav-top` — Menu docks flush under measured chrome
  (`var(--app-sticky-chrome-height)`). Do **not** add `--page-layout-pane-gap`.
- `--page-layout-pane-top` — Pane / Aside rails use chrome + `--page-layout-pane-gap`
  (4rem; same as Container `py-16`).
- `--page-layout-pane-bottom`, `--page-layout-mobile-nav-height`,
  `--page-layout-anchor-offset`, track widths (`--page-layout-pane-width` /
  `--page-layout-aside-width`).
Use `scroll-mt-[var(--page-layout-anchor-offset)]` on in-page anchors so
`scrollIntoView` clears the sticky stack (mobile: chrome + Menu +
`--page-layout-anchor-gap`; lg+: pane-top + gap, Menu height dropped). Use
`tracks="compact"` (11rem for both rails) for dense section rails; Settings keeps
the default 13.75rem for both. `NavListItem` defaults to outward `focus-ring`;
`PageLayoutPane` / `PageLayoutAside` own scrollport padding so rings stay visible.
Action rows use `actions` plus
`nestedInteractiveSuppress.selectableRowSiblingControl` from `@/lib/nested-interactive`.
For a handful of mobile destinations prefer `SegmentedControl` in `paneMobile`;
for longer lists prefer `PageLayoutPaneMobileNav`. Compose breadcrumbs in the
content column below the Menu bar and above the page title. DOM order is
`paneMobile` → `header` → columns (Menu above PageHeader on small viewports).
Sticky for the Menu bar is owned by `PageLayout`'s tall `page-layout-main` /
`pane-mobile` wrapper (opaque `bg-background`, shared `gap` variant — same sticky
model as AppTopBar) — do not `position: fixed` the closed bar. Open overlay owns
Esc dismiss, `RemoveScroll`, FocusScope trap, and `inert` on
`page-layout-header` / `columns` / `footer`. Crossing `lg` force-closes when the
host has no layout box. Overlay `top` tracks the live Menu bar bottom. Scroll-spy
activation offsets must use `usePageLayoutAnchorOffsetPx` from
`@/lib/page-layout-anchor-offset` (live probe of `--page-layout-anchor-offset`) —
do not hardcode mobile/desktop px. Page inset lives on `Container` (`py-16`);
do not re-pad individual routes. Label column uses `appTopBarColumnVariants` from
`app-top-bar` (one SoT with `AppTopBarInner`). Do not put `overflow-x-*` on
`PageLayout`'s `paneMobile` slot. Wrap a scrolling `SegmentedControl` at the call
site instead.

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

## Chip / ChipGroup

Registry interactive `Chip` + `ChipGroup` (M3 filter / assist) are vendored as
`ui/chip.tsx` and `ui/chip-group.tsx`. Supporting helpers:

- `lib/selection-interaction.ts` — Neutral idle + Accent vivid selected ladders
- `lib/disabled-selection.ts` — disabled on/off recolor for selectable controls
- `ui/selection-check.tsx` — includes `drawFrom="toggle"` for Chip hosts

Strip `"use client"`. Import `@/lib/cn` (not Registry `@/lib/utils`). Depends on
`@radix-ui/react-toggle` and `@radix-ui/react-toggle-group`. Prefer Chip for
catalog facet toggles; keep static card labels on `Badge`. Segmented
field·operator·value bars stay Registry `Filters` (not yet required here).

## Held local overrides (discuss before dropping)

### Add to Registry (so console can drop the fork)

| Item | Why |
| --- | --- |
| **TableOfContents `kind: "separator"`** | Apps catalog TOC divider between groups |

### AgentConsole (synced)

Vendored with Registry: Claude / Codex / Cursor skins as `AGENT_CONSOLE_THEME_*`
exports. Setup consumes those constants — do not re-copy palettes at the call
site. Strip `"use client"`; prefer `@/lib/cn` over Registry `@/lib/utils`.

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
