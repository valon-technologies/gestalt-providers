import { createLowlight } from "lowlight";
import bash from "highlight.js/lib/languages/bash";
import javascript from "highlight.js/lib/languages/javascript";
import json from "highlight.js/lib/languages/json";
import typescript from "highlight.js/lib/languages/typescript";
import yaml from "highlight.js/lib/languages/yaml";

import cliLanguage from "@/components/ui/code-block-cli-language";

/**
 * Curated highlight grammars for display CodeBlock (shell, json, yaml, ts/js,
 * plus the CLI command-line grammar). Avoids `lowlight/all`, which pulls every
 * grammar into the main chunk.
 */
export const codeBlockLowlight = createLowlight({
  bash,
  javascript,
  json,
  typescript,
  yaml,
});

codeBlockLowlight.register("cli", cliLanguage);
