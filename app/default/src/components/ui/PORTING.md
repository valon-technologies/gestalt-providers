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
   - accent-vivid → gold-300 bright fill/stroke
   - accent-solid → gold-400 mid control fill (Switch checked)
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

## Button / Input / Field / Label

Registry `button`, `input`, `field`, and `label` are vendored here. Theme
bridges (`--primary`, `--muted`, `--input`, `--disabled*`, `--state-overlay-*`,
`--control-*`) live in `shared/theme.css` + `globals.css` `@theme inline`.
Prefer `@/components/ui/button` and `@/components/ui/input` at call sites;
`@/components/Button` is a legacy adapter (`primary` → `default`,
`danger` → `destructive`).

**One color contract (Registry):** ink is `text-foreground` /
`text-muted-foreground`; fills are `bg-primary` / `bg-muted` / `bg-secondary`.
Do not reintroduce console `text-primary` / `text-muted` / `text-secondary`
ink utilities — those names are Registry fills. Optional console-only
`text-faint` remains for tertiary chrome (40% alpha-dark).

Compose labeled controls with `Field` + `FieldLabel` (+ `FieldDescription` /
`FieldError`) — see Registry `guidelines/fields.md`.

`Input` / `Textarea` expose a `chrome` variant (`standalone` default,
`group` for `InputGroupInput` / `InputGroupTextarea`) so the shell owns the
focus ring without stacking on the inner control.

## Choice cards (RadioGroup)

Do not fork tile chrome at call sites. Import helpers from
`@/lib/choice-card-chrome`. Primitives come from `@/components/ui/radio-group`:

- `choiceCardClassName` + `choiceCardHoverClassName` — simple tiles
- `choiceCardFormShellClassName` + `choiceCardFormFieldsClassName` — nested fields
  (`Collapsible` drawer outside the `Label`, per `nested-interactive.md`)
- `choiceCardRadioClassName` / `choiceCardRadioEyebrowClassName` — radio placement
- Pass `focusRing="none"` on `RadioGroupItem` inside choice cards

Canonical: upstream `choice-card-chrome` + `radio-group` stories. Requires `--accent-solid` in theme (gold-400).

## Code (inline)

Registry `code` is vendored as `ui/code.tsx` (`Code` / `codeVariants`). Use for
inline identifiers / paths / flags in UI copy — not `CodeBlock`, not `Kbd`.
Do not hand-roll `bg-muted font-mono` at call sites.

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

## Card / Collapsible

Registry `card` + `collapsible` are vendored here. Expand/collapse is owned by
`Collapsible` — paint the root with `cardVariants({ variant: "outline" })` at
the call site (cards.md Card Collapsible). Do not restyle trigger hover/press
(List Item Neutral via `listItemInteraction`). Drawer height animation lives on
`[data-slot=collapsible-content]` in `globals.css` (Registry theme keyframes).

## Tabs

Registry `tabs` is vendored as `ui/tabs.tsx` (line underline + sliding
indicator). Use for content navigation; mode switching stays on
`SegmentedControl`. Do not invent gold/brand underline chrome at call sites.

## CodeBlock / code-fence

Registry `code-block` + `code-fence` are vendored here for display snippets
(Build MCP install, etc.). Keep highlighting on lowlight → `.typeset-code-hljs`
(`src/styles/typeset-code-hljs.css`). Do not reintroduce
Shiki for these surfaces. Shell paint maps Registry `bg-muted/50` /
`border-border/50` to console `bg-alpha-5` / `border-alpha`. Multi-file /
language-tab recipes (`MultiFileCodeBlock`, `LanguageTabsCodeBlock`) use
vendored `tabs`.

## Stepper

Registry `stepper` is vendored as `ui/stepper.tsx` (process navigation with
checks + connectors). Depends on `lib/list-item-interaction.ts` and
`selection-check`. Theme bridges include `--accent-fill-hover` /
`--accent-fill-pressed` for soft-selected hover (selectable-rows). Build page
uses controlled `activationMode="jump"` — do not restyle Stepper chrome at the
call site (layout-only wrappers OK).

## PageLayout / NavList

Registry `page-layout` and `nav-list` are vendored here. `PageLayout` owns
in-page geometry (header band, start Pane, content, end Aside); `NavList` is
router-agnostic section navigation for rails, sheets, and flyouts. Set
`--page-layout-pane-top` / `--page-layout-pane-bottom` and track widths
(`--page-layout-pane-width` / `--page-layout-aside-width`) in `globals.css` next
to nav-height tokens — do not scatter `sticky top-*` or hand-roll grid tracks per
page. Use `tracks="compact"` (11rem pane) for dense section rails; Settings keeps
the default 13.75rem pane. `NavListItem` defaults to outward `focus-ring`;
`PageLayoutPane` / `PageLayoutAside` own scrollport padding so rings stay visible.
Action rows use `actions` plus
`nestedInteractiveSuppress.selectableRowSiblingControl` from `@/lib/nested-interactive`.

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
| **Avatar `xl` size** (`size-10` / 40px) | Nav account chip; Registry stops at `lg` |
| **TableOfContents `kind: "separator"`** | Apps catalog TOC divider between groups |
| **`AGENT_CONSOLE_THEME_CODEX` / `_CURSOR` exports** (optional) | Story palettes only today; Build re-copies them — promote from `agent-console.stories` |

### Keep as console adapters (not Registry gaps)

| Item | Why |
| --- | --- |
| **`theme-toggle.tsx`** | Thin wrapper on `@/hooks/use-theme` — Registry ships its own `useTheme`; app theme ownership stays local |
