# Public UI core synchronization

The public app and a private component Registry should be compatible by
construction, not by manually translating changes between two design systems.

The public boundary is [`ui-core.contract.json`](ui-core.contract.json):

- This repository owns the semantic token contract, neutral fallback values,
  and the reviewed public release snapshot.
- A private Registry may own tenant palettes, licensed fonts, type choices,
  and tenant-only component extensions.
- A deployment selects a private `theme.css` at runtime. No tenant asset is
  bundled into this repository.

[`ui-core.lock.json`](ui-core.lock.json) records the exact public closure: its
file hashes and direct npm dependencies are generated from the explicit
`sharedComponentEntries`, their local imports, and the theme-contract roots.
It contains no Registry URL, tenant palette, font, or theme asset.

## Ownership and export direction

Today the private Registry is the authoring home for its **public-safe**
component lane. Its exporter produces a one-way, reviewable pull request into
this repository; this repository does not read from, publish to, or name the
private Registry at runtime. The lockfile verifies the imported snapshot. It
does not discover or perform synchronization by itself.

The clean end state is a standalone public `ui-core` package consumed by both
the public app and the Registry, with the Registry adding private extensions
around it. Until that extraction is worthwhile, the public-safe exporter is
the only permitted direction. Do not maintain two hand-edited copies or rely
on a language model to reconstruct a component.

## Export shape

The Registry exporter should export only the explicitly selected generic
component closure into this repository rather than publish its full private
registry artifact. It should resolve local dependencies, then reject a closure
containing private theme items or deployment-specific assets. A directory is
never implicitly shared merely because it is named `components/ui`.

The resulting public update should contain only generic source files and the
refreshed lockfile, for example:

```text
src/components/ui/<explicit shared primitive>.tsx
src/lib/*.ts
ui-core.lock.json
```

After an intentional public-core update, run `npm run ui-core:lock`, then
`npm run check`. CI verifies the hashes so a copied Registry component cannot
silently drift afterward. The lockfile has no palette values, font family
names, private URLs, or deployment paths.

## Change routing

| Change in the private Registry | Public action |
| --- | --- |
| Palette, font, or a tenant-only token | None. Map it privately in the deployment stylesheet. |
| A generic component behavior or semantic utility change | Export the public-safe closure and open a normal public PR. |
| A new generic semantic role | Version the contract, add neutral light/dark defaults and the Tailwind bridge, update the tenant template, then export components that use it. |

This keeps a brand refresh out of the public release cycle while making a true
design-system API change explicit and reviewable.

## Rules for shared components

1. Use only the semantic utility roles declared by the contract.
2. Keep interaction density, control sizing, and motion in the public app;
   they are component behavior, not a tenant theme API.
3. Split a component from a private typeset or theme dependency before export.
4. Keep tenant-only extensions private instead of adding escape-hatch tokens
   to the public contract.
5. Let an exporter or bot open mechanical sync changes. Do not rely on a
   language model to recreate the same component by hand.
6. Keep app compositions out of the shared list unless the Registry explicitly
   adopts them. In particular, `CopyIconButton`, `CopyableCode`, `CodeBlock`,
   `CodeFence`, and `SectionHeader` are app-owned today; adding one later is an
   intentional contract change, not an incidental folder inclusion.

This gives the Registry one visual vocabulary while preserving a genuinely
generic public artifact.
