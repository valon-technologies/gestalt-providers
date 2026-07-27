import fs from "node:fs";
import { lockPath, serializeUICoreLock } from "./ui-core-lock.mjs";

fs.writeFileSync(lockPath, serializeUICoreLock());
console.log("Wrote ui-core.lock.json");
