/**
 * Optional static SPA + mock API (production `out/`). Prefer Vite DEV via
 * `/local-dev` (`GESTALT_WORKFLOWS_LOCAL_MOCK=1 bunx vite`).
 *
 * Usage: API_PORT=3200 node scripts/serve-workflows-local.mjs
 */
import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  handleWorkflowsLocalMock,
  resolveWorktreeDisplayName,
  SLACK_APP,
} from "./workflows-local-mock.mjs";

const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const outDir = path.join(projectDir, "out");
const port = Number(process.env.PORT || process.env.API_PORT || 8080);
const worktreeName = resolveWorktreeDisplayName();

const contentTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".woff2": "font/woff2",
};

async function readIndexHtml() {
  const html = await fs.readFile(path.join(outDir, "index.html"), "utf8");
  let next = html;
  if (!/<base\b/i.test(next)) {
    next = next.replace(/<head(\s[^>]*)?>/i, (match) => `${match}<base href="/">`);
  }
  return next;
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host}`);
    const pathname = decodeURIComponent(url.pathname);

    if (handleWorkflowsLocalMock(req, res, pathname, url)) {
      return;
    }

    const relativePath =
      pathname === "/" ? "index.html" : pathname.replace(/^\//, "");
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
    } catch {
      // SPA fallback
    }

    const html = await readIndexHtml();
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(html);
  } catch (error) {
    res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
    res.end(error instanceof Error ? error.message : "Internal Server Error");
  }
});

server.listen(port, "127.0.0.1", () => {
  console.log(
    `workflows local mock (static) on http://127.0.0.1:${port} (open /apps/${SLACK_APP}/admin/workflows)` +
      (worktreeName ? `; worktree=${worktreeName}` : ""),
  );
});
