import { readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const adminDir = path.join(projectDir, "public", "admin");

const expectedFiles = [
  "index.html",
  "echarts.min.js",
  "theme.css",
  "fonts/GeistMono_Regular.woff2",
  "fonts/instrument-sans-latin-wght-normal.woff2",
  "fonts/newsreader-opsz72-latin-400-normal.woff2",
];

async function walk(relativeDir = "") {
  const dir = path.join(adminDir, relativeDir);
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const relativePath = relativeDir
      ? `${relativeDir}/${entry.name}`
      : entry.name;
    if (entry.isDirectory()) {
      files.push(...(await walk(relativePath)));
    } else {
      files.push(relativePath);
    }
  }
  return files.sort();
}

async function main() {
  const files = await walk();
  const missing = expectedFiles.filter((file) => !files.includes(file));
  if (missing.length > 0) {
    console.error(`Missing admin static assets: ${missing.join(", ")}`);
    process.exit(1);
  }

  const unexpected = files.filter((file) => !expectedFiles.includes(file));
  if (unexpected.length > 0) {
    console.error(`Unexpected admin static assets: ${unexpected.join(", ")}`);
    process.exit(1);
  }

  for (const file of expectedFiles) {
    const fullPath = path.join(adminDir, file);
    const info = await stat(fullPath);
    if (!info.isFile()) {
      console.error(`Admin asset is not a file: ${file}`);
      process.exit(1);
    }
    await readFile(fullPath);
  }

  console.log(
    `Verified ${expectedFiles.length} admin static assets under public/admin/`,
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
