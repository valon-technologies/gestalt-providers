import { createLowlight } from "lowlight";
import bash from "highlight.js/lib/languages/bash";
import javascript from "highlight.js/lib/languages/javascript";
import json from "highlight.js/lib/languages/json";
import typescript from "highlight.js/lib/languages/typescript";
import yaml from "highlight.js/lib/languages/yaml";

/**
 * Curated highlight grammars for display CodeBlock — matches the docs/registry
 * surface (shell, json, yaml, ts/js). Avoids `lowlight/all`, which pulls every
 * grammar into the main chunk.
 */
export const codeBlockLowlight = createLowlight({
  bash,
  javascript,
  json,
  typescript,
  yaml,
});
