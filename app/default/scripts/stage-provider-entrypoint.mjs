import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const binDir = path.join(projectDir, ".gestaltd", "bin");
const entrypoint = path.join(binDir, "default");

mkdirSync(binDir, { recursive: true });
writeFileSync(
  entrypoint,
  `#!/bin/sh
if [ -n "\${GESTALT_APP_WRITE_CATALOG:-}" ]; then
  cat > "\$GESTALT_APP_WRITE_CATALOG" <<'CATALOG'
name: home
operations:
  - id: static
    method: GET
    transport: app
CATALOG
fi
`,
  { encoding: "utf8", mode: 0o755 },
);
chmodSync(entrypoint, 0o755);
