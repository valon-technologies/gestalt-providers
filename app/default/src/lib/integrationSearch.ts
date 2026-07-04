import type { Integration } from "@/lib/api";

export function getIntegrationLabel(integration: Integration): string {
  return integration.displayName || integration.name;
}

function getSearchableFields(integration: Integration): string[] {
  return [
    integration.name,
    integration.displayName || "",
    integration.description || "",
  ];
}

export function filterIntegrations(
  integrations: Integration[],
  rawQuery: string,
): Integration[] {
  const query = rawQuery.trim().toLowerCase();

  if (!query) {
    return integrations;
  }

  return integrations.filter((integration) =>
    getSearchableFields(integration).some((value) =>
      value.toLowerCase().includes(query),
    ),
  );
}
