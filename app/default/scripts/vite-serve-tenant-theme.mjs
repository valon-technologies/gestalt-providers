import { createReadStream, existsSync, statSync } from "node:fs";
import path from "node:path";

const MIME = {
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".ttf": "font/ttf",
  ".otf": "font/otf",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
};

/**
 * Dev-only: keep the production theme contract on Vite.
 *
 * Production gestaltd serves:
 *   - stylesheet at `/theme.css`
 *   - `assetsDir` recursively at `/theme/<path>`
 *
 * Tenant `@font-face` rules use absolute `/theme/fonts/…` URLs. Mirroring only
 * the CSS (GESTALT_THEME_FILE → @theme.css) without the asset tree makes those
 * requests fall through to the SPA HTML shell — fonts error, system fallbacks
 * render, and the UI looks "wrong" even though tokens applied.
 *
 * Assets root: `GESTALT_THEME_ASSETS_DIR`, else the directory containing
 * `GESTALT_THEME_FILE` (matches deploy `assetsDir: ./ui` + `stylesheet:
 * ./ui/theme.css`).
 *
 * @returns {import('vite').Plugin}
 */
export function serveTenantThemeInDev() {
  const themeFile = process.env.GESTALT_THEME_FILE?.trim();
  const assetsDirEnv = process.env.GESTALT_THEME_ASSETS_DIR?.trim();
  const assetsDir = assetsDirEnv
    ? path.resolve(assetsDirEnv)
    : themeFile
      ? path.dirname(path.resolve(themeFile))
      : null;
  const stylesheet = themeFile ? path.resolve(themeFile) : null;

  return {
    name: "serve-tenant-theme-in-dev",
    apply: "serve",
    configureServer(server) {
      if (!assetsDir && !stylesheet) {
        return;
      }

      console.info(
        `[serve-tenant-theme-in-dev] /theme.css ← ${stylesheet ?? "(none)"}; /theme/* ← ${assetsDir ?? "(none)"}`,
      );

      server.middlewares.use((req, res, next) => {
        const reqPath = (req.url ?? "").split("?")[0];

        if (reqPath === "/theme.css" && stylesheet && existsSync(stylesheet)) {
          res.setHeader("Content-Type", "text/css; charset=utf-8");
          res.setHeader("Cache-Control", "no-cache");
          createReadStream(stylesheet).pipe(res);
          return;
        }

        if (!reqPath.startsWith("/theme/") || !assetsDir) {
          next();
          return;
        }

        const relative = decodeURIComponent(reqPath.slice("/theme/".length));
        if (!relative || relative.includes("\0")) {
          next();
          return;
        }

        const file = path.normalize(path.join(assetsDir, relative));
        const rootWithSep = assetsDir.endsWith(path.sep)
          ? assetsDir
          : `${assetsDir}${path.sep}`;
        if (
          file !== assetsDir &&
          !file.startsWith(rootWithSep)
        ) {
          res.statusCode = 403;
          res.end("Forbidden");
          return;
        }

        if (!existsSync(file) || !statSync(file).isFile()) {
          next();
          return;
        }

        const mime = MIME[path.extname(file).toLowerCase()];
        if (mime) {
          res.setHeader("Content-Type", mime);
        }
        res.setHeader("Cache-Control", "no-cache");
        createReadStream(file).pipe(res);
      });
    },
  };
}
