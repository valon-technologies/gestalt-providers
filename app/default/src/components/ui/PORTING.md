# Porting shared UI kit components

When lifting a shared UI kit control into `src/components/ui/`:

1. **Keep semantic class names** (`bg-accent-subtle`, `text-accent-foreground`,
   `bg-accent-vivid`, …). Do not reinterpret “accent” as “brand-colored text.”
2. **Map only through the theme bridge** in `shared/theme.css` /
   `globals.css`. Those aliases already mean:
   - accent\* → fill (from `--brand-soft`)
   - accent\*-foreground → ink (`--foreground`)
3. **Forbidden on selected chrome:** `data-active:text-brand`,
   `data-[selected]:text-brand`, `data-active:text-gold-*`, and the same for
   `data-[state=active]`. Selected rows use ink on an accent fill.
4. Adapt motion / focus / sizing to local tokens (`focus-ring`,
   `duration-select-*`, control heights) — not color roles.

`oxlint` enforces (3) via `home/no-brand-text-on-selected`
(`oxlint-plugin-home.mjs`, scoped to `src/components/ui/**`).
