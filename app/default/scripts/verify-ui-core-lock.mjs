import assert from "node:assert/strict";
import fs from "node:fs";
import { lockPath, serializeUICoreLock } from "./ui-core-lock.mjs";

assert(fs.existsSync(lockPath), "ui-core.lock.json is missing; run npm run ui-core:lock");
assert.equal(
  fs.readFileSync(lockPath, "utf8"),
  serializeUICoreLock(),
  "ui-core.lock.json is stale; run npm run ui-core:lock",
);

console.log("UI core lock verified");
