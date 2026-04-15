"use client";

import { createElement, useEffect, useId, useRef, useState } from "react";
import type { KeyboardEvent, MouseEvent, ReactNode } from "react";
import {
  Integration,
  PENDING_CONNECTION_PATH,
  resolveAPIPath,
  startIntegrationOAuth,
  connectManualIntegration,
  disconnectIntegration,
} from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import Button from "./Button";
import { CheckCircleIcon, GearIcon, DefaultIcon } from "./icons";
import IntegrationSettingsModal from "./IntegrationSettingsModal";

const SAFE_SVG_ELEMENTS = new Set([
  "clipPath",
  "circle",
  "defs",
  "ellipse",
  "feColorMatrix",
  "feComponentTransfer",
  "feComposite",
  "feFlood",
  "feFuncA",
  "filter",
  "g",
  "image",
  "line",
  "linearGradient",
  "mask",
  "path",
  "polygon",
  "polyline",
  "radialGradient",
  "rect",
  "stop",
  "svg",
  "title",
  "use",
]);

const SAFE_SVG_ATTRIBUTES = new Set([
  "aria-label",
  "aria-labelledby",
  "clip-path",
  "clip-rule",
  "color-interpolation-filters",
  "cx",
  "cy",
  "d",
  "fill",
  "fill-opacity",
  "fill-rule",
  "filter",
  "flood-color",
  "gradientTransform",
  "gradientUnits",
  "height",
  "href",
  "id",
  "in",
  "in2",
  "mask",
  "offset",
  "opacity",
  "operator",
  "points",
  "preserveAspectRatio",
  "r",
  "result",
  "role",
  "rx",
  "ry",
  "stop-color",
  "stop-opacity",
  "stroke",
  "stroke-linecap",
  "stroke-linejoin",
  "stroke-miterlimit",
  "stroke-opacity",
  "stroke-width",
  "tableValues",
  "transform",
  "type",
  "viewBox",
  "width",
  "x",
  "x1",
  "x2",
  "xlink:href",
  "xmlns",
  "y",
  "y1",
  "y2",
]);

function normalizeSVGAttrName(name: string): string {
  if (name === "class") return "className";
  if (name.startsWith("aria-") || name.startsWith("data-")) {
    return name;
  }
  return name.replace(/[:\-]([a-z])/g, (_, letter: string) =>
    letter.toUpperCase(),
  );
}

function isSafeSVGHref(value: string): boolean {
  const normalized = value.replace(/\s/g, "").toLowerCase();
  return normalized.startsWith("#") || normalized.startsWith("data:image/");
}

function buildSVGIDMap(root: Element, prefix: string): Map<string, string> {
  const ids = new Map<string, string>();
  let index = 0;
  for (const element of [root, ...Array.from(root.querySelectorAll("[id]"))]) {
    const currentID = element.getAttribute("id");
    if (!currentID) continue;
    ids.set(currentID, `${prefix}-${index}`);
    index += 1;
  }
  return ids;
}

function rewriteSVGReferences(value: string, idMap: Map<string, string>): string {
  let rewritten = value.replace(/url\(#([^)]+)\)/g, (match, id: string) => {
    const mappedID = idMap.get(id);
    return mappedID ? `url(#${mappedID})` : match;
  });
  if (rewritten.startsWith("#")) {
    const mappedID = idMap.get(rewritten.slice(1));
    if (mappedID) {
      rewritten = `#${mappedID}`;
    }
  }
  return rewritten;
}

function renderSafeSVGNode(
  node: ChildNode,
  key: string,
  idMap: Map<string, string>,
): ReactNode | null {
  if (node.nodeType === Node.TEXT_NODE) {
    const text = node.textContent?.trim();
    return text ? text : null;
  }
  if (node.nodeType !== Node.ELEMENT_NODE) {
    return null;
  }

  const element = node as Element;
  const tagName = element.tagName;
  if (!SAFE_SVG_ELEMENTS.has(tagName)) {
    return null;
  }

  const props: Record<string, string> = { key };
  for (const attr of Array.from(element.attributes)) {
    if (!SAFE_SVG_ATTRIBUTES.has(attr.name)) {
      continue;
    }

    let value =
      attr.name === "id"
        ? idMap.get(attr.value) ?? attr.value
        : rewriteSVGReferences(attr.value, idMap);
    if ((attr.name === "href" || attr.name === "xlink:href") && !isSafeSVGHref(value)) {
      continue;
    }
    props[normalizeSVGAttrName(attr.name)] = value;
  }

  if (tagName === "svg") {
    props["aria-hidden"] = "true";
    props.focusable = "false";
  }

  const children: ReactNode[] = [];
  Array.from(element.childNodes).forEach((child, index) => {
    const rendered = renderSafeSVGNode(child, `${key}-${index}`, idMap);
    if (rendered !== null) {
      children.push(rendered);
    }
  });
  return createElement(tagName, props, ...children);
}

function renderSafeIcon(svg: string, prefix: string): ReactNode | null {
  const doc = new DOMParser().parseFromString(svg, "image/svg+xml");
  const root = doc.documentElement;
  if (root.nodeName !== "svg" || doc.querySelector("parsererror")) {
    return null;
  }
  return renderSafeSVGNode(root, prefix, buildSVGIDMap(root, prefix));
}

function hasConnectionParams(integration: Integration): boolean {
  return (
    !!integration.connectionParams &&
    Object.keys(integration.connectionParams).length > 0
  );
}

function hasSettingsControls(integration: Integration): boolean {
  return (
    !!integration.connected ||
    (integration.authTypes?.length ?? 0) > 0 ||
    (integration.connections?.length ?? 0) > 0
  );
}

type ConnectionTarget = {
  instance?: string;
  connection?: string;
};

type PendingSelection = {
  action: string;
  pendingToken: string;
};

export default function IntegrationCard({
  integration,
  onConnected,
  onDisconnected,
}: {
  integration: Integration;
  onConnected?: () => void;
  onDisconnected?: () => void;
}) {
  const [loading, setLoading] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [showParamForm, setShowParamForm] = useState(false);
  const [pendingOAuthTarget, setPendingOAuthTarget] = useState<ConnectionTarget>({});
  const [pendingSelection, setPendingSelection] = useState<PendingSelection | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const pendingSelectionFormRef = useRef<HTMLFormElement>(null);
  const iconIDPrefix = `provider-icon-${useId().replace(/:/g, "")}`;

  const iconNode = integration.iconSvg
    ? renderSafeIcon(integration.iconSvg, iconIDPrefix)
    : null;
  const needsParams = hasConnectionParams(integration);
  const mountedPath = integration.mountedPath?.trim();
  const settingsAvailable = hasSettingsControls(integration);
  const mountedAccessible = mountedPath ? integration.mountedAccessible !== false : false;
  const cardNavigationEnabled = !!mountedPath && mountedAccessible && !settingsOpen && !showParamForm;

  useEffect(() => {
    if (!pendingSelection) return;
    pendingSelectionFormRef.current?.submit();
  }, [pendingSelection]);

  function collectConnectionParams(
    form: HTMLFormElement,
  ): Record<string, string> {
    const params: Record<string, string> = {};
    if (!integration.connectionParams) return params;
    for (const name of Object.keys(integration.connectionParams)) {
      const val = (new FormData(form).get(`cp_${name}`) as string)?.trim();
      if (val) params[name] = val;
    }
    return params;
  }

  async function beginOAuth(connectionParams?: Record<string, string>, target: ConnectionTarget = pendingOAuthTarget) {
    setLoading(true);
    setError(null);
    try {
      const { url } = await startIntegrationOAuth(
        integration.name,
        undefined,
        connectionParams,
        target.instance,
        target.connection,
      );
      window.location.href = url;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to start OAuth");
      setLoading(false);
    }
  }

  async function handleStartOAuth(instance?: string, connection?: string) {
    const target = { instance, connection };
    setPendingOAuthTarget(target);
    if (needsParams && !showParamForm) {
      setSettingsOpen(false);
      setShowParamForm(true);
      setError(null);
      return;
    }
    await beginOAuth(undefined, target);
  }

  async function handleSubmitToken(credential: string | Record<string, string>, connectionParams?: Record<string, string>, instance?: string, connection?: string) {
    setSubmitting(true);
    setError(null);
    try {
      const result = await connectManualIntegration(
        integration.name, credential, connectionParams, instance, connection,
      );
      if (result.status === "selection_required") {
        if (!result.pendingToken) {
          throw new Error("Connection requires selection, but the server did not return a pending token.");
        }
        setSettingsOpen(false);
        setPendingSelection({
          action: resolveAPIPath(result.selectionUrl || PENDING_CONNECTION_PATH),
          pendingToken: result.pendingToken,
        });
      } else {
        setSettingsOpen(false);
        onConnected?.();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to connect");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleParamSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const params = collectConnectionParams(e.currentTarget);
    await beginOAuth(params);
  }

  function handleCancelForm() {
    setShowParamForm(false);
    setPendingOAuthTarget({});
    setError(null);
  }

  async function handleDisconnect(instance?: string, connection?: string) {
    setDisconnecting(true);
    setError(null);
    try {
      await disconnectIntegration(integration.name, instance, connection);
      onDisconnected?.();
      setSettingsOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to disconnect");
    } finally {
      setDisconnecting(false);
    }
  }

  function handleSettingsClose() {
    setSettingsOpen(false);
    setError(null);
  }

  function navigateToMountedPath() {
    if (!mountedPath) return;
    window.location.assign(mountedPath);
  }

  function handleCardClick(e: MouseEvent<HTMLDivElement>) {
    if (!cardNavigationEnabled) return;
    const target = e.target as HTMLElement | null;
    if (target?.closest("button, a, input, textarea, select, label, form")) {
      return;
    }
    navigateToMountedPath();
  }

  function handleCardKeyDown(e: KeyboardEvent<HTMLDivElement>) {
    if (!cardNavigationEnabled || e.target !== e.currentTarget) return;
    if (e.key !== "Enter" && e.key !== " ") return;
    e.preventDefault();
    navigateToMountedPath();
  }

  function renderConnectionParamFields() {
    if (!integration.connectionParams) return null;
    return Object.entries(integration.connectionParams).map(([name, def]) => (
      <div key={name} className="mt-3">
        <label
          htmlFor={`cp_${name}-${integration.name}`}
          className="label-text block"
        >
          {def.description || name}
        </label>
        <input
          id={`cp_${name}-${integration.name}`}
          name={`cp_${name}`}
          type="text"
          required={def.required}
          defaultValue={def.default}
          placeholder={name}
          className={`mt-1.5 w-full ${INPUT_CLASSES}`}
        />
      </div>
    ));
  }

  return (
    <div
      data-testid={`integration-card-${integration.name}`}
      className={`rounded-lg border border-alpha bg-base-white p-6 transition-all duration-150 dark:bg-surface ${
        cardNavigationEnabled
          ? "cursor-pointer hover:border-alpha-strong hover:shadow-card focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold-400 focus-visible:ring-offset-2 focus-visible:ring-offset-base-white dark:focus-visible:ring-offset-surface"
          : "hover:border-alpha-strong hover:shadow-card"
      }`}
      onClick={handleCardClick}
      onKeyDown={handleCardKeyDown}
      role={cardNavigationEnabled ? "link" : undefined}
      tabIndex={cardNavigationEnabled ? 0 : undefined}
      aria-label={
        cardNavigationEnabled
          ? `Open ${integration.displayName || integration.name}`
          : undefined
      }
    >
      {pendingSelection && (
        <form
          ref={pendingSelectionFormRef}
          method="post"
          action={pendingSelection.action}
          className="hidden"
        >
          <input
            type="hidden"
            name="pending_token"
            value={pendingSelection.pendingToken}
          />
        </form>
      )}
      <div className="flex items-start justify-between">
        <div className="flex items-start gap-3">
          <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-base-100 text-muted [&>svg]:h-5 [&>svg]:w-5 dark:bg-surface-raised">
            {iconNode ?? <DefaultIcon />}
          </div>
          <div>
            <h3 className="text-base font-heading font-semibold text-primary">
              {integration.displayName || integration.name}
            </h3>
            {integration.description && (
              <p className="mt-1 line-clamp-2 text-sm text-muted">
                {integration.description}
              </p>
            )}
          </div>
        </div>
        <div className="flex items-center gap-1">
          {integration.connected && (
            <CheckCircleIcon className="h-5 w-5 text-grove-500" />
          )}
          {settingsAvailable && (
            <button
              onClick={(event) => {
                event.stopPropagation();
                setSettingsOpen(true);
              }}
              className="flex h-8 w-8 items-center justify-center rounded-md text-faint transition-all duration-150 hover:bg-alpha-5 hover:text-muted"
              aria-label={`${integration.displayName || integration.name} settings`}
            >
              <GearIcon className="h-4 w-4" />
            </button>
          )}
        </div>
      </div>
      {error && !settingsOpen && (
        <p className="mt-3 text-sm text-ember-500">{error}</p>
      )}
      {showParamForm && (
        <form onSubmit={handleParamSubmit} className="mt-4">
          {renderConnectionParamFields()}
          <div className="mt-4 flex gap-2">
            <Button type="submit" disabled={loading}>
              {loading ? "Connecting..." : "Connect"}
            </Button>
            <Button
              type="button"
              variant="secondary"
              onClick={handleCancelForm}
              disabled={loading}
            >
              Cancel
            </Button>
          </div>
        </form>
      )}
      {settingsOpen && (
        <IntegrationSettingsModal
          integration={integration}
          onClose={handleSettingsClose}
          onStartOAuth={handleStartOAuth}
          onSubmitToken={handleSubmitToken}
          onDisconnect={handleDisconnect}
          reconnecting={loading}
          disconnecting={disconnecting}
          submitting={submitting}
          error={error}
        />
      )}
    </div>
  );
}
