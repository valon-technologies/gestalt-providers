# Porting shared UI kit components

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
3. **Forbidden on selected chrome:** `data-active:text-brand`,
   `data-[selected]:text-brand`, `data-active:text-gold-*`, and the same for
   `data-[state=active]`. Selected rows use ink on an accent fill.
4. Adapt motion / focus / sizing to local tokens (`focus-ring`,
   `duration-select-*`, control heights) — not color roles.

`oxlint` enforces (3) via `home/no-brand-text-on-selected`
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

## Choice cards (RadioGroup)

Do not fork tile chrome at call sites. Import `choiceCardClassName` from
`@/components/RadioGroup` (Registry ChoiceCards / ChoiceCardsGrid recipe).
Layout-only changes are OK. Agent contract: [`../../../AGENTS.md`](../../../AGENTS.md).

## Code (inline)

Registry `code` is vendored as `ui/code.tsx` (`Code` / `codeVariants`). Use for
inline identifiers / paths / flags in UI copy — not `CodeBlock`, not `Kbd`.
Do not hand-roll `bg-muted font-mono` at call sites.

## Brand type scale

Registry PageHeader / SectionHeader consume `text-heading-*`, `text-display-*`,
`tracking-heading`, `tracking-display` (valon.ai/style). Bridge those tokens in
`shared/theme.css` (`--valon-text-*`, `--valon-tracking-*`) and expose them via
`globals.css` `@theme inline`. Do not invent freestyle `tracking-*` /
`text-*` sizes at call sites.

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
(`src/styles/typeset-code-hljs.css` from valon-typeset). Do not reintroduce
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

## Held local overrides (discuss before dropping)

### Add to Registry (so console can drop the fork)

| Item | Why |
| --- | --- |
| **Avatar `xl` size** (`size-10` / 40px) | Nav account chip; Registry stops at `lg` |
| **TableOfContents `kind: "separator"`** | Apps catalog TOC divider between groups |
| **`InputGroupInput` forwards `ref`** | Search bar focus; Registry component does not forward ref today |
| **`AGENT_CONSOLE_THEME_CODEX` / `_CURSOR` exports** (optional) | Story palettes only today; Build re-copies them — promote from `agent-console.stories` |

### Keep as console adapters (not Registry gaps)

| Item | Why |
| --- | --- |
| **`theme-toggle.tsx`** | Thin wrapper on `@/hooks/use-theme` — Registry ships its own `useTheme`; app theme ownership stays local |