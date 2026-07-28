import { getAuthInfo } from "@/lib/api";

const SESSION_KEY = "gestalt.workflow.provider";

let memoryProvider: string | null = null;
let resolving: Promise<string> | null = null;

/** Remember a provider observed from platform workflow runs or config. */
export function rememberWorkflowProvider(provider: string | undefined): void {
  const name = provider?.trim();
  if (!name) return;
  memoryProvider = name;
  try {
    sessionStorage.setItem(SESSION_KEY, name);
  } catch {
    // Private browsing / disabled storage — memory cache still helps per tab.
  }
}

function readCachedProvider(): string | null {
  const named = memoryProvider?.trim();
  if (named) return named;
  try {
    const stored = sessionStorage.getItem(SESSION_KEY)?.trim();
    if (stored) {
      memoryProvider = stored;
      return stored;
    }
  } catch {
    return null;
  }
  return null;
}

/**
 * Resolve the configured workflow platform provider for this deployment.
 * Prefer an explicit override, then session cache, then auth/info features.
 */
export async function resolveWorkflowProvider(
  explicit?: string,
): Promise<string> {
  const named = explicit?.trim() || readCachedProvider();
  if (named) return named;

  if (!resolving) {
    resolving = loadWorkflowProviderFromAuthInfo().finally(() => {
      resolving = null;
    });
  }
  return resolving;
}

async function loadWorkflowProviderFromAuthInfo(): Promise<string> {
  const info = await getAuthInfo();
  const fromServer = info.features?.workflowDefaultProvider?.trim();
  if (fromServer) {
    rememberWorkflowProvider(fromServer);
    return fromServer;
  }

  // Interim until gestaltd exposes features.workflowDefaultProvider on all
  // deployments. valon.tools configures a single workflow provider named "local".
  const interim = "local";
  rememberWorkflowProvider(interim);
  return interim;
}
