import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectDir = path.dirname(fileURLToPath(import.meta.url));

// In dev, GESTALT_THEME_FILE re-points the layout's @theme.css import at a
// live tenant stylesheet (e.g. a deployment-repo checkout) so theme edits
// hot-reload. The source is mirrored into .dev/theme.css rather than aliased
// directly: Turbopack can only resolve and watch files inside the project
// root, and widening the root to reach a sibling checkout makes it watch
// multi-gigabyte trees (see THEMING.md). `npm run dev:theme` keeps the
// mirror live; this seed copy makes `next dev` alone work with a snapshot.
// Production always bundles the empty stub — tenant themes are a serve-time
// concern.
const devThemeSource =
  process.env.NODE_ENV !== "production" && process.env.GESTALT_THEME_FILE
    ? path.resolve(process.env.GESTALT_THEME_FILE)
    : null;
const devThemeMirror = path.join(projectDir, ".dev", "theme.css");
if (devThemeSource) {
  fs.mkdirSync(path.dirname(devThemeMirror), { recursive: true });
  // copyFileSync writes *through* an existing symlink; a leftover symlink at
  // the mirror whose target leaves the project root re-creates the Turbopack
  // out-of-root OOM (ISS-20260612-003). Remove unconditionally so the mirror
  // is always a regular file.
  fs.rmSync(devThemeMirror, { force: true });
  fs.copyFileSync(devThemeSource, devThemeMirror);
}
const themeTarget = devThemeSource
  ? "./.dev/theme.css"
  : "./src/app/theme.stub.css";

/** @type {import('next').NextConfig} */
const nextConfig = {
  turbopack: {
    // Pin the root: Turbopack otherwise infers it from the *outermost*
    // lockfile, and a stray one above the checkout silently widens the watch
    // scope to sibling repos (ISS-20260612-003). Never widen this.
    root: projectDir,
    resolveAlias: { "@theme.css": themeTarget },
  },
  webpack: (config) => {
    config.resolve.alias["@theme.css"] = path.resolve(projectDir, themeTarget);
    return config;
  },
  // Rewrites are incompatible with the static export, so both are gated:
  // `next build` exports as before, `next dev` proxies API calls to a
  // local gestaltd (dev.sh) instead.
  ...(process.env.NODE_ENV === "production"
    ? { output: "export" }
    : {
        async rewrites() {
          const api =
            process.env.GESTALT_API_URL ||
            `http://localhost:${process.env.API_PORT || 8080}`;
          return [
            { source: "/api/:path*", destination: `${api}/api/:path*` },
            // Prod-parity loop for the serve-time theme: the layout's
            // /theme.css link resolves against a local gestaltd instead of
            // 404ing in dev. The @theme.css mirror remains the HMR loop.
            { source: "/theme.css", destination: `${api}/theme.css` },
            { source: "/theme/:path*", destination: `${api}/theme/:path*` },
          ];
        },
      }),
};

export default nextConfig;
