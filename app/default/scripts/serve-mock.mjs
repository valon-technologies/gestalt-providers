import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const outDir = path.join(projectDir, "out");
const port = Number(process.env.PORT || process.env.API_PORT || 8080);
const testMountPath = "/portal";

const contentTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".woff2": "font/woff2",
};

async function readIndexHtml(baseHref) {
  const html = await fs.readFile(path.join(outDir, "index.html"), "utf8");
  if (!baseHref || /<base\b/i.test(html)) {
    return html;
  }
  return html.replace(/<head(\s[^>]*)?>/i, (match) => `${match}<base href="${baseHref}">`);
}

const indexHtmlByBase = new Map();

function indexHtmlFor(baseHref) {
  if (!indexHtmlByBase.has(baseHref)) {
    indexHtmlByBase.set(baseHref, readIndexHtml(baseHref));
  }
  return indexHtmlByBase.get(baseHref);
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host}`);
    const pathname = decodeURIComponent(url.pathname);
    const isMounted = pathname === testMountPath || pathname.startsWith(`${testMountPath}/`);
    const appPath = isMounted
      ? pathname.slice(testMountPath.length) || "/"
      : pathname;

    // Production gestaltd owns these endpoints. The mock server intentionally
    // mirrors its unconfigured response rather than letting the SPA fallback
    // turn a stylesheet request into HTML.
    if (appPath === "/theme.css") {
      res.writeHead(200, { "Content-Type": "text/css; charset=utf-8" });
      res.end();
      return;
    }
    if (appPath === "/brand.json") {
      res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
      res.end("{}");
      return;
    }
    if (appPath.startsWith("/theme/")) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Not Found");
      return;
    }

    const relativePath =
      appPath === "/" ? "index.html" : appPath.replace(/^\//, "");
    const filePath = path.join(outDir, relativePath);

    if (!filePath.startsWith(outDir)) {
      res.writeHead(403).end("Forbidden");
      return;
    }

    try {
      const stat = await fs.stat(filePath);
      if (stat.isFile()) {
        const ext = path.extname(filePath);
        const body = await fs.readFile(filePath);
        res.writeHead(200, {
          "Content-Type": contentTypes[ext] || "application/octet-stream",
        });
        res.end(body);
        return;
      }
      if (stat.isDirectory()) {
        const indexPath = path.join(filePath, "index.html");
        const indexStat = await fs.stat(indexPath);
        if (indexStat.isFile()) {
          const body = await fs.readFile(indexPath);
          res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
          res.end(body);
          return;
        }
      }
    } catch {
      // Fall through to SPA shell.
    }

    const html = await indexHtmlFor(isMounted ? null : "/");
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(html);
  } catch (error) {
    res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
    res.end(error instanceof Error ? error.message : "Internal Server Error");
  }
});

server.listen(port, "127.0.0.1", () => {
  console.log(`mock SPA server listening on http://127.0.0.1:${port}`);
});
