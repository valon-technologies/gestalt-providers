import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const publicDir = path.join(projectDir, "public");
const indexHtmlPath = path.join(projectDir, "index.html");

const expectedFiles = [
  "favicon.svg",
  "favicon-32x32.png",
  "favicon-48x48.png",
  "apple-touch-icon.png",
];

const expectedLinkTags = [
  '<link rel="icon" href="/favicon.svg" type="image/svg+xml" />',
  '<link rel="icon" href="/favicon-32x32.png" type="image/png" sizes="32x32" />',
  '<link rel="icon" href="/favicon-48x48.png" type="image/png" sizes="48x48" />',
  '<link rel="apple-touch-icon" href="/apple-touch-icon.png" sizes="180x180" />',
];

async function main() {
  for (const file of expectedFiles) {
    const fullPath = path.join(publicDir, file);
    const info = await stat(fullPath);
    if (!info.isFile()) {
      console.error(`Favicon asset is not a file: ${file}`);
      process.exit(1);
    }
    await readFile(fullPath);
  }

  const indexHtml = await readFile(indexHtmlPath, "utf8");
  const missingTags = expectedLinkTags.filter((tag) => !indexHtml.includes(tag));
  if (missingTags.length > 0) {
    console.error(`index.html is missing favicon link tags:\n${missingTags.join("\n")}`);
    process.exit(1);
  }

  const svg = await readFile(path.join(publicDir, "favicon.svg"), "utf8");
  if (!svg.includes('aria-label="Gestalt"')) {
    console.error("favicon.svg is missing the Gestalt aria-label");
    process.exit(1);
  }

  console.log(
    `Verified ${expectedFiles.length} favicon assets under public/ and index.html link tags`,
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
