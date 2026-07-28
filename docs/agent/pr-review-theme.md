# PR review — theme pass

Use with `/pr-review` or manual review when a PR touches `app/default/**/*.css`,
`shared/theme.css`, or component styling.

Full rules: [`theme-boundary.md`](theme-boundary.md). Human contract:
[`app/default/THEMING.md`](../../app/default/THEMING.md).

## Checklist

- [ ] Semantic tokens only in components (`bg-card`, `text-muted-foreground`, `text-display-sm`)
- [ ] No deployment palette constants in `shared/theme.css` or `globals.css`
- [ ] No `var(--tenant-*)` (or org-prefixed palette) bridges in `@theme inline`
- [ ] No legacy gestalt-shell tokens (`border-alpha`, `bg-base-white`, `text-faint`, `bg-surface`)
- [ ] No raw Tailwind palette utilities or hardcoded hex in component code
- [ ] Fill + foreground pairs matched (`bg-card` + `text-card-foreground`)
- [ ] New UI matches token vocabulary in `components/ui/*`
- [ ] Missing Registry utilities get generic defaults here; tenant values in deployment `deploy/ui/theme.css`

## High-severity patterns

```text
--tenant-*              in shared/theme.css or globals.css
var(--tenant-*)         in globals.css @theme inline
@import ... theme.css    from deployment or Registry paths
@font-face              for licensed brand fonts in public bundle
```

## Detection

```bash
BASE=$(git merge-base main HEAD)
git diff "$BASE"...HEAD -- app/default/shared/theme.css app/default/src/globals.css \
  | rg '--tenant-|var\(--[a-z]+-(text|tracking)-'
```
