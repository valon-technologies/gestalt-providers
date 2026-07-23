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
  isAPIErrorStatus,
  selectAppAdminRegistryVersion,
  type AppAdminRegistryResponse,
} from "@/lib/api";

const APPS_PATH = "/apps";
const POLL_INTERVAL_MS = 12_000;

function initialSelectedVersion(registry: AppAdminRegistryResponse): string {
  const published = registry.publishedVersions.map((version) => version.version);
  if (registry.desiredVersion && published.includes(registry.desiredVersion)) {
    return registry.desiredVersion;
  }
  return published[0] ?? "";
}

export default function AppAdminPageClient({ appName }: { appName: string }) {
  useDocumentTitle(`${appName} · App management`);
  const [registry, setRegistry] = useState<AppAdminRegistryResponse | null>(null);
  const [selectedVersion, setSelectedVersion] = useState("");
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [forbidden, setForbidden] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loadRequestIdRef = useRef(0);

  const loadRegistry = useCallback(async () => {
    const requestId = loadRequestIdRef.current + 1;
    loadRequestIdRef.current = requestId;

    try {
      const data = await getAppAdminRegistry(appName);
      if (loadRequestIdRef.current !== requestId) return data;
      setRegistry(data);
      setSelectedVersion((current) => {
        const next = initialSelectedVersion(data);
        if (!current || !data.publishedVersions.some((v) => v.version === current)) {
          return next;
        }
        return current;
      });
      setForbidden(false);
      setError(null);
      return data;
    } catch (err) {
      if (loadRequestIdRef.current !== requestId) return;
      if (isAPIErrorStatus(err, 403)) {
        setForbidden(true);
        setRegistry(null);
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

  async function handleSelectVersion() {
    if (!selectedVersion || registry?.selectionDisabled) return;

    setSubmitting(true);
    setError(null);
    try {
      await selectAppAdminRegistryVersion(appName, selectedVersion);
      await loadRegistry();
    } catch (err) {
      if (isAPIErrorStatus(err, 409)) {
        await loadRegistry();
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to select version");
    } finally {
      setSubmitting(false);
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
                selectedVersion={selectedVersion}
                onSelectedVersionChange={setSelectedVersion}
                onSelectVersion={() => void handleSelectVersion()}
                submitting={submitting}
                error={error}
              />
            </div>
          ) : null}
        </Container>
      </div>
    </AuthGuard>
  );
}
