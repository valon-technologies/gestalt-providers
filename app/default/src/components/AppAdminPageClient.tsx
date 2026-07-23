import { useCallback, useEffect, useRef, useState } from "react";
import { Link } from "@tanstack/react-router";
import AuthGuard from "@/components/AuthGuard";
import Container from "@/components/Container";
import Nav from "@/components/Nav";
import { AppAdminVersionPanel } from "@/features/registry/app-admin-version-panel";
import { isActiveRegistryRollout } from "@/features/registry/format";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  getAppAdminRegistry,
  getIntegrations,
  isAPIErrorStatus,
  selectAppAdminRegistryVersion,
  type AppAdminRegistryResponse,
} from "@/lib/api";

const APPS_PATH = "/apps";
const POLL_INTERVAL_MS = 12_000;

export default function AppAdminPageClient({ appName }: { appName: string }) {
  useDocumentTitle(`${appName} · App management`);
  const [registry, setRegistry] = useState<AppAdminRegistryResponse | null>(null);
  const [appMountedPath, setAppMountedPath] = useState<string | undefined>();
  const [deployingVersion, setDeployingVersion] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [forbidden, setForbidden] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loadRequestIdRef = useRef(0);

  const loadRegistry = useCallback(async () => {
    const requestId = loadRequestIdRef.current + 1;
    loadRequestIdRef.current = requestId;

    try {
      const [data, integrations] = await Promise.all([
        getAppAdminRegistry(appName),
        getIntegrations().catch(() => []),
      ]);
      if (loadRequestIdRef.current !== requestId) return data;
      setRegistry(data);
      const mountedPath = integrations
        .find((integration) => integration.name === appName)
        ?.mountedPath?.trim();
      setAppMountedPath(mountedPath || undefined);
      setForbidden(false);
      setError(null);
      return data;
    } catch (err) {
      if (loadRequestIdRef.current !== requestId) return;
      if (isAPIErrorStatus(err, 403)) {
        setForbidden(true);
        setRegistry(null);
        setAppMountedPath(undefined);
        setError(null);
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load app registry");
      return;
    } finally {
      if (loadRequestIdRef.current === requestId) {
        setLoading(false);
      }
    }
  }, [appName]);

  useEffect(() => {
    setLoading(true);
    setForbidden(false);
    setRegistry(null);
    setAppMountedPath(undefined);
    setError(null);
    void loadRegistry();
  }, [loadRegistry]);

  useEffect(() => {
    if (!registry) return undefined;
    const shouldPoll =
      registry.selectionDisabled ||
      (registry.rollout ? isActiveRegistryRollout(registry.rollout.state) : false);
    if (!shouldPoll) return undefined;

    const timer = window.setTimeout(() => {
      void loadRegistry();
    }, POLL_INTERVAL_MS);
    return () => window.clearTimeout(timer);
  }, [registry, loadRegistry]);

  async function handleDeployVersion(version: string) {
    if (!version || registry?.selectionDisabled) return;

    setDeployingVersion(version);
    setError(null);
    try {
      await selectAppAdminRegistryVersion(appName, version);
      await loadRegistry();
    } catch (err) {
      if (isAPIErrorStatus(err, 409)) {
        await loadRegistry();
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to deploy version");
    } finally {
      setDeployingVersion(null);
    }
  }

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <Container as="main" className="py-12">
          <div className="mb-8 animate-fade-in-up">
            <Link
              to={APPS_PATH}
              className="text-sm text-muted transition-colors hover:text-primary"
            >
              ← Back to apps
            </Link>
          </div>

          {loading ? (
            <p className="text-sm text-muted">Loading app registry…</p>
          ) : forbidden ? (
            <div
              className="animate-fade-in-up rounded-2xl border border-alpha bg-base-white p-6 dark:bg-surface"
              data-testid="app-admin-access-denied"
            >
              <h1 className="text-2xl font-heading text-primary">Access denied</h1>
              <p className="mt-3 text-sm text-muted">
                You do not have permission to manage this app.
              </p>
            </div>
          ) : error && !registry ? (
            <p className="text-sm text-ember-500">{error}</p>
          ) : registry ? (
            <div className="animate-fade-in-up [animation-delay:60ms]">
              <AppAdminVersionPanel
                registry={registry}
                appMountedPath={appMountedPath}
                deployingVersion={deployingVersion}
                onDeployVersion={(version) => void handleDeployVersion(version)}
                error={error}
              />
            </div>
          ) : null}
        </Container>
      </div>
    </AuthGuard>
  );
}
