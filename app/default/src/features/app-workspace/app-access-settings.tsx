import { useEffect, useMemo, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import type { AppAccessOperation, AppAccessProfile } from "@/lib/api";
import { formatOperationResourceLabel } from "@/lib/operationGroups";
import { queryKeys } from "@/lib/query-keys";
import { updateAppAccess } from "@/lib/api";
import { useAppAccessQuery } from "@/lib/queries";

function operationLabel(operation: AppAccessOperation): string {
  const title = operation.title?.trim();
  if (title) return title;
  return operation.id
    .split(/[._-]/g)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function operationGroup(operation: AppAccessOperation): string {
  return operation.id.split(".")[0] || operation.id;
}

function profileOperations(profile: AppAccessProfile | undefined): string[] {
  return profile?.enabledOperations ?? [];
}

export default function AppAccessSettings({
  appName,
  connected,
}: {
  appName: string;
  connected: boolean;
}) {
  const queryClient = useQueryClient();
  const accessQuery = useAppAccessQuery(appName, { enabled: connected });
  const [draft, setDraft] = useState<string[]>([]);

  useEffect(() => {
    setDraft(profileOperations(accessQuery.data));
  }, [accessQuery.data]);

  const enabled = useMemo(() => new Set(draft), [draft]);
  const initial = useMemo(
    () => profileOperations(accessQuery.data),
    [accessQuery.data],
  );
  const dirty = useMemo(
    () =>
      draft.length !== initial.length ||
      draft.some((operation) => !initial.includes(operation)),
    [draft, initial],
  );
  const groups = useMemo(() => {
    const grouped = new Map<string, AppAccessOperation[]>();
    for (const operation of accessQuery.data?.operations ?? []) {
      const group = operationGroup(operation);
      const current = grouped.get(group);
      if (current) current.push(operation);
      else grouped.set(group, [operation]);
    }
    return [...grouped.entries()].sort(([a], [b]) => a.localeCompare(b));
  }, [accessQuery.data?.operations]);

  const saveMutation = useMutation({
    mutationFn: () => updateAppAccess(appName, draft),
    onSuccess: (profile) => {
      queryClient.setQueryData(queryKeys.integrations.access(appName), profile);
    },
  });

  function setOperation(operation: string, value: boolean) {
    setDraft((current) => {
      const next = new Set(current);
      if (value) next.add(operation);
      else next.delete(operation);
      return [...next].sort();
    });
    saveMutation.reset();
  }

  function resetToDefaults() {
    const defaults =
      accessQuery.data?.operations
        .filter((operation) => operation.default)
        .map((operation) => operation.id) ?? [];
    setDraft(defaults);
    saveMutation.reset();
  }

  return (
    <Card aria-label="App capabilities" data-testid="app-access-settings">
      <CardHeader>
        <CardTitle>Capabilities</CardTitle>
        <CardDescription>
          Choose what Gestalt can do with this connected account. These settings
          apply to the app, CLI, and MCP.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {!connected ? (
          <p className="text-sm text-muted-foreground">
            Connect an account above to manage its capabilities.
          </p>
        ) : accessQuery.isPending ? (
          <div className="space-y-3" aria-label="Loading capabilities">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
        ) : accessQuery.error ? (
          <Alert variant="destructive">
            <AlertTitle>Capabilities unavailable</AlertTitle>
            <AlertDescription className="flex flex-wrap items-center gap-3">
              <span>We couldn't load the access settings for this app.</span>
              <Button
                size="sm"
                variant="outline"
                onClick={() => void accessQuery.refetch()}
              >
                Try again
              </Button>
            </AlertDescription>
          </Alert>
        ) : groups.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            This app has not published any configurable capabilities yet.
          </p>
        ) : (
          <>
            <p className="text-xs text-muted-foreground">
              Read-only capabilities are enabled by default when the app marks
              them as safe. Turn on write capabilities only when you need them.
            </p>
            <div className="space-y-5">
              {groups.map(([group, operations]) => (
                <section key={group} aria-labelledby={`app-access-${group}`}>
                  <h3
                    id={`app-access-${group}`}
                    className="mb-2 text-sm font-medium"
                  >
                    {formatOperationResourceLabel(group)}
                  </h3>
                  <div className="divide-y divide-border rounded-md border border-border">
                    {operations.map((operation) => {
                      const checked = enabled.has(operation.id);
                      return (
                        <label
                          key={operation.id}
                          className="flex cursor-pointer items-center justify-between gap-4 px-3 py-3 first:rounded-t-md last:rounded-b-md hover:bg-muted/40"
                        >
                          <span className="min-w-0 space-y-1">
                            <span className="flex flex-wrap items-center gap-2 text-sm font-medium">
                              <span>{operationLabel(operation)}</span>
                              {operation.readOnly ? (
                                <Badge variant="secondary">Read-only</Badge>
                              ) : null}
                              {operation.default ? (
                                <Badge variant="outline">Default</Badge>
                              ) : null}
                            </span>
                            {operation.description ? (
                              <span className="block text-xs text-muted-foreground">
                                {operation.description}
                              </span>
                            ) : null}
                            <span className="block font-mono text-[11px] text-muted-foreground-soft">
                              {operation.id}
                            </span>
                          </span>
                          <Switch
                            checked={checked}
                            onCheckedChange={(value) =>
                              setOperation(operation.id, value)
                            }
                            aria-label={`${checked ? "Disable" : "Enable"} ${operationLabel(operation)}`}
                          />
                        </label>
                      );
                    })}
                  </div>
                </section>
              ))}
            </div>
            <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border pt-4">
              <span className="text-xs text-muted-foreground">
                {saveMutation.isSuccess
                  ? "Saved"
                  : accessQuery.data?.defaultsInitialized
                    ? "Your custom settings are active."
                    : "Using the app's recommended defaults."}
              </span>
              <div className="flex gap-2">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={resetToDefaults}
                  disabled={!dirty || saveMutation.isPending}
                >
                  Reset defaults
                </Button>
                <Button
                  size="sm"
                  onClick={() => saveMutation.mutate()}
                  disabled={!dirty || saveMutation.isPending}
                >
                  {saveMutation.isPending ? "Saving…" : "Save capabilities"}
                </Button>
              </div>
            </div>
            {saveMutation.error ? (
              <Alert variant="destructive">
                <AlertTitle>Couldn't save capabilities</AlertTitle>
                <AlertDescription>
                  Try again. Your current connection is unchanged.
                </AlertDescription>
              </Alert>
            ) : null}
          </>
        )}
      </CardContent>
    </Card>
  );
}
