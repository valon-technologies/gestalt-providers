import type { Integration, IntegrationPrompt } from "@/lib/api";

type PromptOverrideMap = Record<string, IntegrationPrompt[]>;

/**
 * Optional local override of deployer-owned prompts for prod-remote / local
 * preview when the remote API has not yet shipped `home.config.prompts`.
 *
 * Drop `src/dev/promptOverrides.local.json` (gitignored) with the same shape
 * as `apps.home.config.prompts` values: `{ [appName]: [{ id, text }, ...] }`.
 *
 * Only imported from the integrations query in DEV so production bundles omit
 * this module.
 */
const overrideModules = import.meta.glob<PromptOverrideMap>(
  "../dev/promptOverrides.local.json",
  { eager: true, import: "default" },
);

function readDevPromptOverrides(): PromptOverrideMap | null {
  const overrides = Object.values(overrideModules)[0];
  return overrides && typeof overrides === "object" ? overrides : null;
}

/** Merge local prompt overrides onto integrations (DEV query path only). */
export function applyDevPromptOverrides(
  integrations: Integration[],
): Integration[] {
  const overrides = readDevPromptOverrides();
  if (!overrides) return integrations;

  return integrations.map((integration) => {
    const prompts = overrides[integration.name];
    if (!prompts?.length) return integration;
    return { ...integration, prompts };
  });
}
