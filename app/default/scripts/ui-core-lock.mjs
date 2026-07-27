import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
export const lockPath = path.join(projectDir, "ui-core.lock.json");

function walk(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(directory, entry.name);
    return entry.isDirectory() ? walk(entryPath) : [entryPath];
  });
}

function sourceFilesAt(relativePath) {
  const absolutePath = path.join(projectDir, relativePath);
  assert(fs.existsSync(absolutePath), `shared source root does not exist: ${relativePath}`);
  const stat = fs.statSync(absolutePath);
  if (stat.isFile()) {
    return [absolutePath];
  }
  return walk(absolutePath).filter((file) => /\.(?:ts|tsx|css)$/.test(file));
}

function importedSpecifiers(source) {
  const specifiers = new Set();
  const importPattern = /\b(?:import|export)\s+(?:[^"']*?\s+from\s+)?["']([^"']+)["']/g;
  const cssImportPattern = /@import\s+["']([^"']+)["']/g;
  for (const pattern of [importPattern, cssImportPattern]) {
    for (const match of source.matchAll(pattern)) {
      specifiers.add(match[1]);
    }
  }
  return specifiers;
}

function localSourceFile(importer, specifier) {
  if (!specifier.startsWith(".") && !specifier.startsWith("@/")) return null;

  const unresolved = specifier.startsWith("@/")
    ? path.join(projectDir, "src", specifier.slice(2))
    : path.resolve(path.dirname(importer), specifier);
  const candidates = [
    unresolved,
    ...[".ts", ".tsx", ".css"].map((extension) => `${unresolved}${extension}`),
    ...["index.ts", "index.tsx", "index.css"].map((file) => path.join(unresolved, file)),
  ];
  const resolved = candidates.find(
    (candidate) => fs.existsSync(candidate) && fs.statSync(candidate).isFile(),
  );
  assert(resolved, `cannot resolve local shared source import ${specifier} from ${path.relative(projectDir, importer)}`);
  return resolved;
}

function sharedComponentClosure(entries) {
  const files = new Set(entries.flatMap(sourceFilesAt));
  const pending = [...files];

  while (pending.length > 0) {
    const file = pending.pop();
    for (const specifier of importedSpecifiers(fs.readFileSync(file, "utf8"))) {
      const dependency = localSourceFile(file, specifier);
      if (dependency && !files.has(dependency)) {
        files.add(dependency);
        pending.push(dependency);
      }
    }
  }

  return [...files];
}

function packageName(specifier) {
  if (specifier.startsWith(".") || specifier.startsWith("@/")) {
    return null;
  }
  if (specifier.startsWith("@")) {
    return specifier.split("/").slice(0, 2).join("/");
  }
  return specifier.split("/")[0];
}

function importedPackages(source) {
  const specifiers = new Set();
  for (const specifier of importedSpecifiers(source)) {
    const name = packageName(specifier);
    if (name) {
      specifiers.add(name);
    }
  }
  return specifiers;
}

function sha256(file) {
  return `sha256:${createHash("sha256").update(fs.readFileSync(file)).digest("hex")}`;
}

export function createUICoreLock() {
  const contract = JSON.parse(
    fs.readFileSync(path.join(projectDir, "ui-core.contract.json"), "utf8"),
  );
  const packageJson = JSON.parse(fs.readFileSync(path.join(projectDir, "package.json"), "utf8"));
  const coreFiles = [
    ...new Set([
      ...contract.themeContractRoots.flatMap(sourceFilesAt),
      ...sharedComponentClosure(contract.sharedComponentEntries),
    ]),
  ].sort();
  assert(coreFiles.length > 0, "shared source roots must resolve to source files");

  const files = {};
  const imported = new Set();
  for (const file of coreFiles) {
    files[path.relative(projectDir, file)] = sha256(file);
    for (const dependency of importedPackages(fs.readFileSync(file, "utf8"))) {
      imported.add(dependency);
    }
  }

  const availableDependencies = {
    ...packageJson.dependencies,
    ...packageJson.devDependencies,
  };
  const dependencies = {};
  for (const dependency of [...imported].sort()) {
    assert(availableDependencies[dependency], `missing package version for ${dependency}`);
    dependencies[dependency] = availableDependencies[dependency];
  }

  return {
    schema: "gestalt-ui-core-lock/v1",
    contract: {
      schema: contract.schema,
      version: contract.version,
    },
    files,
    dependencies,
  };
}

export function serializeUICoreLock() {
  return `${JSON.stringify(createUICoreLock(), null, 2)}\n`;
}
