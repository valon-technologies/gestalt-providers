"use client";

import Link from "next/link";
import { useEffect, useState, type ReactNode } from "react";
import { CheckIcon, CopyIcon } from "@/components/icons";

const FALLBACK_ORIGIN = "https://your-gestalt-host";

const mcpTabs = [
  { id: "mcp-claude-code", label: "Claude Code" },
  { id: "mcp-codex", label: "Codex" },
  { id: "mcp-cursor", label: "Cursor" },
  { id: "mcp-other", label: "Other Clients" },
] as const;

type McpTabId = (typeof mcpTabs)[number]["id"];

const mcpTabIds = mcpTabs.map((tab) => tab.id);
const defaultMcpTabId: McpTabId = "mcp-claude-code";

export function GettingStartedDocsPage() {
  const origin = useDeploymentOrigin();

  return (
    <>
      <DocsPageHeader
        eyebrow="Getting Started"
        title="Getting Started"
        description={
          <>
            This guide covers the user-facing workflows for the Gestalt
            workspace you are currently using: install{" "}
            <code className="font-mono text-sm text-primary">gestalt</code>,
            point it at this workspace, sign in when required, connect
            plugins, invoke operations, mint API tokens, and attach an
            MCP-aware client. No command-line experience is required. Follow
            the pages below and copy the commands as-is.
          </>
        }
      />
      <DocsPageBody>
        <div className="rounded-xl border border-alpha bg-base-100 p-5 dark:bg-surface">
          <p className="text-xs font-medium uppercase tracking-[0.16em] text-faint">
            Base URL
          </p>
          <p className="mt-2 font-mono text-sm text-primary">{origin}</p>
        </div>
        <Subheading id="install" title="Install" />
        <p className="doc-copy">
          End users only need the{" "}
          <code className="font-mono text-sm text-primary">gestalt</code> CLI.
        </p>
        <p className="doc-copy">
          The recommended way to install is with{" "}
          <a
            href="https://brew.sh"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            Homebrew
          </a>
          , a free package manager for macOS and Linux. If you do not have
          Homebrew yet, visit{" "}
          <a
            href="https://brew.sh"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            brew.sh
          </a>{" "}
          to install it first.
        </p>
        <p className="doc-copy">
          Tap the Gestalt Homebrew repository first, then install the CLI.
        </p>
        <CodeBlock
          code={`brew tap valon-technologies/gestalt
brew install valon-technologies/gestalt/gestalt`}
        />
        <p className="doc-copy">
          If you prefer a direct download, use the{" "}
          <a
            href="https://github.com/valon-technologies/gestalt/releases"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            GitHub releases page
          </a>{" "}
          and place the binary on your{" "}
          <code className="font-mono text-sm text-primary">PATH</code>.
        </p>

        <Subheading id="point-cli" title="Point the CLI at this workspace" />
        <p className="doc-copy">
          The CLI needs the base URL for your Gestalt workspace. Use either the
          setup wizard or a direct config command.
        </p>
        <SetupMethodTabs
          items={[
            {
              id: "setup-init",
              label: "gestalt init",
              code: "gestalt init",
              description:
                "Interactive setup that stores the URL, can create a project-local `.gestalt/config.json`, and can start browser login.",
            },
            {
              id: "setup-config-set",
              label: "gestalt config set url",
              code: `gestalt config set url ${origin}`,
              description:
                "Writes the user-local CLI config file for this machine.",
            },
            {
              id: "setup-env-var",
              label: "GESTALT_URL",
              code: `export GESTALT_URL=${origin}`,
              description:
                "Per-shell override when you do not want to change stored config.",
            },
          ]}
        />
        <p className="doc-copy">
          The optional{" "}
          <code className="font-mono text-sm text-primary">
            .gestalt/config.json
          </code>{" "}
          file stores only the base URL. The CLI searches the current directory
          and then parent directories until it finds the nearest project
          config.
        </p>
        <div className="doc-copy space-y-2">
          <p>Resolution order:</p>
          <ol className="list-decimal space-y-1 pl-6">
            <li>
              <code className="font-mono text-sm text-primary">--url</code>
            </li>
            <li>
              <code className="font-mono text-sm text-primary">
                GESTALT_URL
              </code>
            </li>
            <li>
              project-local{" "}
              <code className="font-mono text-sm text-primary">
                .gestalt/config.json
              </code>
            </li>
            <li>
              user-local CLI config file, for example{" "}
              <code className="font-mono text-sm text-primary">
                ~/.config/gestalt/config.json
              </code>
            </li>
          </ol>
        </div>

        <Subheading id="authenticate" title="Authenticate" />
        <p className="doc-copy">
          Use browser login for interactive sessions, or set a token directly
          for scripts and other non-interactive clients. If authentication is
          disabled, you can skip both flows and call the API directly.
        </p>
        <AuthMethodTabs
          items={[
            {
              id: "auth-browser",
              label: "gestalt auth",
              code: "gestalt auth login",
              description:
                "Opens your browser for sign-in and then confirms the current session.",
            },
            {
              id: "auth-token",
              label: "GESTALT_API_KEY",
              code: "export GESTALT_API_KEY=gst_api_your_token_here",
              description:
                "Uses an API token directly for scripts, MCP clients, and other non-interactive flows.",
            },
          ]}
        />
        <p className="doc-copy">Then verify access:</p>
        <CodeBlock code="gestalt plugins list" />

        <Subheading id="workflows" title="Inspect workflows" />
        <p className="doc-copy">
          After your workspace URL and auth are set, use{" "}
          <code className="font-mono text-sm text-primary">gestalt workflows</code>{" "}
          to inspect schedules, event triggers, and recent runs from the CLI.
        </p>
        <CodeBlock
          code={`gestalt workflows --help
gestalt workflows schedules list
gestalt workflows triggers list
gestalt workflows runs list`}
        />
        <p className="doc-copy">
          For a deeper walkthrough, open{" "}
          <Link href="/docs/workflows" className="doc-link">
            Workflows
          </Link>
          . If you prefer the browser, the same workflow surfaces are also available on{" "}
          <Link href="/workflows" className="doc-link">
            Workflows
          </Link>
          .
        </p>
      </DocsPageBody>
    </>
  );
}

export function ConnectDocsPage() {
  return (
    <>
      <DocsPageHeader
        eyebrow="Connect Plugins"
        title="Connect Plugins"
        description="Inspect available plugins first, then connect the ones you need."
      />
      <DocsPageBody>
        <p className="doc-copy">
          Plugins available in this workspace appear in both the CLI and the
          UI. Use either surface to start the underlying OAuth or manual
          credential flow.
        </p>
        <CodeBlock
          code={`gestalt plugins list
gestalt plugins connect <plugin>
gestalt plugins connect <plugin> --connection <name> --instance <instance>`}
        />
        <p className="doc-copy">
          If you prefer the browser flow, the same work is available on{" "}
          <Link href="/integrations" className="doc-link">
            Plugins
          </Link>
          .
        </p>
      </DocsPageBody>
    </>
  );
}

export function InvokeDocsPage() {
  const origin = useDeploymentOrigin();

  return (
    <>
      <DocsPageHeader
        eyebrow="Invoke Operations"
        title="Invoke Operations"
        description="Use the catalog built into Gestalt to discover a plugin's operations before making requests."
      />
      <DocsPageBody>
        <InvokeMethodTabs origin={origin} />
      </DocsPageBody>
    </>
  );
}

export function TokensDocsPage() {
  return (
    <>
      <DocsPageHeader
        eyebrow="Manage API Tokens"
        title="Manage API Tokens"
        description="User tokens work for both the HTTP API and the MCP endpoint."
      />
      <DocsPageBody>
        <CodeBlock
          code={`gestalt tokens create --name automation
gestalt tokens list
gestalt tokens revoke <token-id>`}
        />
        <p className="doc-copy">
          Tokens can also be created from{" "}
          <Link href="/authorization" className="doc-link">
            Authorization
          </Link>
          . The raw token value is shown once, so store it immediately in your
          secret manager or shell environment.
        </p>
      </DocsPageBody>
    </>
  );
}

export function WorkflowsDocsPage() {
  return (
    <>
      <DocsPageHeader
        eyebrow="Workflows"
        title="Manage Workflows"
        description="Use the workflow CLI to inspect scheduled work, manage event-driven automation, and review run history without leaving the terminal."
      />
      <DocsPageBody>
        <p className="doc-copy">
          Start by checking the commands exposed by the CLI installed on your machine.
          Different builds may expose different workflow subcommands, so{" "}
          <code className="font-mono text-sm text-primary">--help</code> is the
          fastest source of truth.
        </p>

        <Subheading id="wf-help" title="Start with help" />
        <CodeBlock code="gestalt workflows --help" />
        <p className="doc-copy">
          In this workspace, the main entry points are schedules for delayed
          execution, triggers for event-driven execution, and runs for reviewing
          recent execution history.
        </p>

        <Subheading id="wf-schedules" title="Manage schedules" />
        <p className="doc-copy">
          Workflow schedules invoke a plugin operation later on a cron. Use the same
          parameter syntax as{" "}
          <code className="font-mono text-sm text-primary">gestalt plugins invoke</code>
          : plain strings with{" "}
          <code className="font-mono text-sm text-primary">key=value</code> or JSON
          with{" "}
          <code className="font-mono text-sm text-primary">key:=json</code>.
        </p>
        <CodeBlock
          code={`gestalt workflows schedules list
gestalt workflows schedules list --plugin <plugin>
gestalt workflows schedules get <schedule-id>

gestalt workflows schedules create \\
  --cron "0 */5 * * *" \\
  --timezone "UTC" \\
  --plugin <plugin> \\
  --operation <operation> \\
  -p key=value

gestalt workflows schedules pause <schedule-id>
gestalt workflows schedules resume <schedule-id>
gestalt workflows schedules delete <schedule-id>`}
        />
        <p className="doc-copy">
          Use{" "}
          <code className="font-mono text-sm text-primary">--format json</code>{" "}
          when you want to script against the output.
        </p>

        <Subheading id="wf-triggers" title="Manage event triggers" />
        <p className="doc-copy">
          Event triggers run a plugin operation when an incoming event matches
          a type, source, or subject filter. Use them when a webhook or other
          event stream should start the workflow immediately instead of on a cron.
        </p>
        <CodeBlock
          code={`gestalt workflows triggers list
gestalt workflows triggers list --plugin <plugin>
gestalt workflows triggers list --type <event-type>
gestalt workflows triggers get <trigger-id>

gestalt workflows triggers create \\
  --type <event-type> \\
  --source <event-source> \\
  --subject <event-subject> \\
  --plugin <plugin> \\
  --operation <operation> \\
  -p key=value

gestalt workflows triggers pause <trigger-id>
gestalt workflows triggers resume <trigger-id>
gestalt workflows triggers delete <trigger-id>`}
        />

        <Subheading id="wf-runs" title="Inspect runs" />
        <p className="doc-copy">
          Run history tells you whether scheduled or event-driven work actually
          executed, what plugin and operation ran, and whether the run succeeded
          or failed.
        </p>
        <CodeBlock
          code={`gestalt workflows runs list
gestalt workflows runs list --plugin <plugin>
gestalt workflows runs get <run-id>`}
        />
        <p className="doc-copy">
          The browser surface at{" "}
          <Link href="/workflows" className="doc-link">
            Workflows
          </Link>{" "}
          shows schedules, event triggers, and recent runs if you want a UI for inspection.
        </p>
      </DocsPageBody>
    </>
  );
}

export function McpDocsPage() {
  const origin = useDeploymentOrigin();

  return (
    <>
      <DocsPageHeader
        eyebrow="Use With MCP"
        title="Use With MCP"
        description="Gestalt exposes a single MCP endpoint that gives AI tools access to all your connected plugins. If authentication is enabled, create an API token on the API Tokens page first."
      />
      <DocsPageBody>
        <p className="doc-copy">
          On workspaces with authentication disabled, omit the bearer-token flag
          and header blocks shown below.
        </p>
        <InfoTable
          rows={[
            ["Endpoint", `${origin}/mcp`],
            [
              "Authentication",
              "Authorization: Bearer gst_api_... when auth is enabled",
            ],
            [
              "If no tools appear",
              "Confirm that the plugin is MCP-enabled and connected for your user.",
            ],
          ]}
        />
        <McpClientTabs origin={origin} />
      </DocsPageBody>
    </>
  );
}

export function TroubleshootingDocsPage() {
  return (
    <>
      <DocsPageHeader
        eyebrow="Troubleshooting"
        title="Troubleshooting"
        description="Most user-facing problems come down to the wrong URL, expired auth, or ambiguous connection selection."
      />
      <DocsPageBody>
        <Subheading
          id="ts-not-authenticated"
          title="The CLI says you are not authenticated"
        />
        <p className="doc-copy">
          Run{" "}
          <code className="font-mono text-sm text-primary">
            gestalt auth login
          </code>
          , or set{" "}
          <code className="font-mono text-sm text-primary">
            GESTALT_API_KEY
          </code>{" "}
          if you are using a token directly.
        </p>

        <Subheading
          id="ts-multiple-connections"
          title="A plugin has multiple connections"
        />
        <p className="doc-copy">
          Pass{" "}
          <code className="font-mono text-sm text-primary">--connection</code>{" "}
          or{" "}
          <code className="font-mono text-sm text-primary">--instance</code> so
          Gestalt can resolve the correct credentials.
        </p>

        <Subheading
          id="ts-empty-tools"
          title="The MCP endpoint is mounted, but the tool list is empty"
        />
        <p className="doc-copy">
          That usually means the plugin is available in the workspace config
          but has not been connected for your current user yet.
        </p>
      </DocsPageBody>
    </>
  );
}

function DocsPageHeader({
  eyebrow,
  title,
  description,
}: {
  eyebrow: string;
  title: string;
  description: ReactNode;
}) {
  return (
    <header className="scroll-mt-24 border-b border-alpha pb-10 animate-fade-in-up">
      <p className="text-xs font-medium uppercase tracking-[0.16em] text-faint">
        {eyebrow}
      </p>
      <h1 className="mt-5 font-heading text-4xl font-bold tracking-[-0.03em] text-primary sm:text-5xl">
        {title}
      </h1>
      <div className="mt-6 max-w-3xl text-base leading-7 text-secondary">
        {description}
      </div>
    </header>
  );
}

function DocsPageBody({ children }: { children: ReactNode }) {
  return (
    <div className="mt-8 space-y-5 animate-fade-in-up [animation-delay:60ms]">
      {children}
    </div>
  );
}

function useHashTab(ids: readonly string[], fallbackId: string) {
  const [activeId, setActiveId] = useState(fallbackId);

  useEffect(() => {
    function syncFromHash() {
      const hash = window.location.hash.replace(/^#/, "");
      if (ids.includes(hash)) {
        setActiveId(hash);
      } else if (!hash) {
        setActiveId(fallbackId);
      }
    }

    syncFromHash();
    window.addEventListener("hashchange", syncFromHash);
    return () => window.removeEventListener("hashchange", syncFromHash);
  }, [fallbackId, ids]);

  function selectTab(id: string) {
    setActiveId(id);
    const url = new URL(window.location.href);
    url.hash = id;
    window.history.replaceState(null, "", url);
  }

  return [activeId, selectTab] as const;
}

function SetupMethodTabs({
  items,
}: {
  items: { id: string; label: string; code: string; description: string }[];
}) {
  const [activeId, setActiveId] = useState(items[0]?.id ?? "");

  return (
    <div className="space-y-4">
      <div
        role="tablist"
        aria-label="CLI setup methods"
        className="flex flex-wrap gap-5 border-b border-alpha"
      >
        {items.map((item) => {
          const isActive = item.id === activeId;
          return (
            <button
              key={item.id}
              id={item.id}
              type="button"
              role="tab"
              aria-selected={isActive}
              aria-controls={`${item.id}-panel`}
              onClick={() => setActiveId(item.id)}
              className={`-mb-px border-b-2 px-1 pb-3 pt-1 text-sm font-medium transition-colors duration-150 ${
                isActive
                  ? "border-gold-600 text-primary dark:border-gold-300"
                  : "border-transparent text-muted hover:border-base-300 hover:text-primary dark:hover:border-base-600"
              }`}
            >
              {item.label}
            </button>
          );
        })}
      </div>

      {items.map((item) => {
        const isActive = item.id === activeId;
        return (
          <section
            key={item.id}
            id={`${item.id}-panel`}
            role="tabpanel"
            aria-labelledby={item.id}
            hidden={!isActive}
            className={
              isActive
                ? "space-y-4 rounded-b-xl border-x border-b border-alpha bg-base-100 px-5 py-5 dark:bg-surface"
                : "hidden"
            }
          >
            <CodeBlock code={item.code} />
            <p className="doc-copy">{item.description}</p>
          </section>
        );
      })}
    </div>
  );
}

function AuthMethodTabs({
  items,
}: {
  items: { id: string; label: string; code: string; description: string }[];
}) {
  const [activeId, setActiveId] = useState(items[0]?.id ?? "");

  return (
    <div className="space-y-4">
      <div
        role="tablist"
        aria-label="Authentication methods"
        className="flex flex-wrap gap-5 border-b border-alpha"
      >
        {items.map((item) => {
          const isActive = item.id === activeId;
          return (
            <button
              key={item.id}
              id={item.id}
              type="button"
              role="tab"
              aria-selected={isActive}
              aria-controls={`${item.id}-panel`}
              onClick={() => setActiveId(item.id)}
              className={`-mb-px border-b-2 px-1 pb-3 pt-1 text-sm font-medium transition-colors duration-150 ${
                isActive
                  ? "border-gold-600 text-primary dark:border-gold-300"
                  : "border-transparent text-muted hover:border-base-300 hover:text-primary dark:hover:border-base-600"
              }`}
            >
              {item.label}
            </button>
          );
        })}
      </div>

      {items.map((item) => {
        const isActive = item.id === activeId;
        return (
          <section
            key={item.id}
            id={`${item.id}-panel`}
            role="tabpanel"
            aria-labelledby={item.id}
            hidden={!isActive}
            className={
              isActive
                ? "space-y-4 rounded-b-xl border-x border-b border-alpha bg-base-100 px-5 py-5 dark:bg-surface"
                : "hidden"
            }
          >
            <CodeBlock code={item.code} />
            <p className="doc-copy">{item.description}</p>
          </section>
        );
      })}
    </div>
  );
}

function InvokeMethodTabs({ origin }: { origin: string }) {
  const [activeId, setActiveId] = useState<"invoke-cli" | "invoke-http">(
    "invoke-cli",
  );

  return (
    <div className="space-y-4">
      <div
        role="tablist"
        aria-label="Invocation methods"
        className="flex flex-wrap gap-5 border-b border-alpha"
      >
        {[
          { id: "invoke-cli", label: "CLI" },
          { id: "invoke-http", label: "HTTP" },
        ].map((item) => {
          const isActive = item.id === activeId;
          return (
            <button
              key={item.id}
              id={item.id}
              type="button"
              role="tab"
              aria-selected={isActive}
              aria-controls={`${item.id}-panel`}
              onClick={() =>
                setActiveId(item.id as "invoke-cli" | "invoke-http")
              }
              className={`-mb-px border-b-2 px-1 pb-3 pt-1 text-sm font-medium transition-colors duration-150 ${
                isActive
                  ? "border-gold-600 text-primary dark:border-gold-300"
                  : "border-transparent text-muted hover:border-base-300 hover:text-primary dark:hover:border-base-600"
              }`}
            >
              {item.label}
            </button>
          );
        })}
      </div>

      <section
        id="invoke-cli-panel"
        role="tabpanel"
        aria-labelledby="invoke-cli"
        hidden={activeId !== "invoke-cli"}
        className={
          activeId === "invoke-cli"
            ? "space-y-4 rounded-b-xl border-x border-b border-alpha bg-base-100 px-5 py-5 dark:bg-surface"
            : "hidden"
        }
      >
        <CodeBlock
          code={`gestalt plugins invoke <plugin>
gestalt plugins describe <plugin> <operation>
gestalt plugins invoke <plugin> <operation> -p key=value
gestalt plugins invoke <plugin> <operation> -p filters:='{"status":"open"}'
gestalt plugins invoke <plugin> <operation> --input-file payload.json --select data.items`}
        />
        <p className="doc-copy">
          If you omit the operation,{" "}
          <code className="font-mono text-sm text-primary">
            gestalt plugins invoke &lt;plugin&gt;
          </code>{" "}
          lists available operations instead of running one.
        </p>
      </section>

      <section
        id="invoke-http-panel"
        role="tabpanel"
        aria-labelledby="invoke-http"
        hidden={activeId !== "invoke-http"}
        className={
          activeId === "invoke-http"
            ? "space-y-4 rounded-b-xl border-x border-b border-alpha bg-base-100 px-5 py-5 dark:bg-surface"
            : "hidden"
        }
      >
        <p className="doc-copy">
          The CLI calls the same HTTP API that the workspace exposes for direct
          programmatic access. The API keeps{" "}
          <code className="font-mono text-sm text-primary">integrations</code>{" "}
          in its route paths even though the CLI uses{" "}
          <code className="font-mono text-sm text-primary">plugins</code>.
        </p>
        <CodeBlock
          code={`curl \\
  -H "Authorization: Bearer $GESTALT_API_KEY" \\
  ${origin}/api/v1/integrations

curl \\
  -H "Authorization: Bearer $GESTALT_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"example":"value"}' \\
  ${origin}/api/v1/<plugin>/<operation>`}
        />
      </section>
    </div>
  );
}

function McpClientTabs({ origin }: { origin: string }) {
  const [activeTabId, setActiveTabId] = useHashTab(mcpTabIds, defaultMcpTabId);

  return (
    <div className="space-y-5">
      <div
        role="tablist"
        aria-label="MCP client configuration"
        className="flex flex-wrap gap-5 border-b border-alpha"
      >
        {mcpTabs.map((tab) => {
          const isActive = tab.id === activeTabId;
          return (
            <button
              key={tab.id}
              id={tab.id}
              type="button"
              role="tab"
              aria-selected={isActive}
              aria-controls={`${tab.id}-panel`}
              onClick={() => setActiveTabId(tab.id)}
              className={`-mb-px border-b-2 px-1 pb-3 pt-1 text-sm font-medium transition-colors duration-150 ${
                isActive
                  ? "border-gold-600 text-primary dark:border-gold-300"
                  : "border-transparent text-muted hover:border-base-300 hover:text-primary dark:hover:border-base-600"
              }`}
            >
              {tab.label}
            </button>
          );
        })}
      </div>

      <section
        id="mcp-claude-code-panel"
        role="tabpanel"
        aria-labelledby="mcp-claude-code"
        hidden={activeTabId !== "mcp-claude-code"}
        className={
          activeTabId === "mcp-claude-code"
            ? "space-y-5 rounded-b-xl border-x border-b border-alpha bg-base-100 px-5 py-5 dark:bg-surface"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Use{" "}
          <code className="font-mono text-sm text-primary">.mcp.json</code>{" "}
          for a project-scoped workspace shared in version control, or{" "}
          <code className="font-mono text-sm text-primary">~/.claude.json</code>{" "}
          for a private local or user-scoped config.
        </p>
        <CodeBlock
          code={`{
  "mcpServers": {
    "gestalt": {
      "type": "http",
      "url": "${origin}/mcp",
      "headers": {
        "Authorization": "Bearer \${GESTALT_API_KEY}"
      }
    }
  }
}`}
        />
        <p className="doc-copy">Or add it from the CLI:</p>
        <CodeBlock
          code={`claude mcp add --transport http --scope project --header "Authorization: Bearer $GESTALT_API_KEY" gestalt ${origin}/mcp`}
        />
      </section>

      <section
        id="mcp-codex-panel"
        role="tabpanel"
        aria-labelledby="mcp-codex"
        hidden={activeTabId !== "mcp-codex"}
        className={
          activeTabId === "mcp-codex"
            ? "space-y-5 rounded-b-xl border-x border-b border-alpha bg-base-100 px-5 py-5 dark:bg-surface"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Codex can register the workspace directly from the CLI:
        </p>
        <CodeBlock
          code={`codex mcp add gestalt --url ${origin}/mcp --bearer-token-env-var GESTALT_API_KEY`}
        />
        <p className="doc-copy">
          If authentication is disabled, omit{" "}
          <code className="font-mono text-sm text-primary">
            --bearer-token-env-var GESTALT_API_KEY
          </code>{" "}
          from the command.
        </p>
      </section>

      <section
        id="mcp-cursor-panel"
        role="tabpanel"
        aria-labelledby="mcp-cursor"
        hidden={activeTabId !== "mcp-cursor"}
        className={
          activeTabId === "mcp-cursor"
            ? "space-y-5 rounded-b-xl border-x border-b border-alpha bg-base-100 px-5 py-5 dark:bg-surface"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Config file:{" "}
          <code className="font-mono text-sm text-primary">.cursor/mcp.json</code>{" "}
          in your project root, or{" "}
          <code className="font-mono text-sm text-primary">~/.cursor/mcp.json</code>{" "}
          globally.
        </p>
        <CodeBlock
          code={`{
  "mcpServers": {
    "gestalt": {
      "url": "${origin}/mcp",
      "headers": {
        "Authorization": "Bearer \${env:GESTALT_API_KEY}"
      }
    }
  }
}`}
        />
      </section>

      <section
        id="mcp-other-panel"
        role="tabpanel"
        aria-labelledby="mcp-other"
        hidden={activeTabId !== "mcp-other"}
        className={
          activeTabId === "mcp-other"
            ? "space-y-5 rounded-b-xl border-x border-b border-alpha bg-base-100 px-5 py-5 dark:bg-surface"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Any MCP-compatible client can connect to Gestalt. You need three
          pieces of information:
        </p>
        <InfoTable
          rows={[
            ["URL", `${origin}/mcp`],
            [
              "Header",
              "Authorization: Bearer gst_api_... when auth is enabled",
            ],
            ["Config key", "usually mcpServers"],
          ]}
        />
        <CodeBlock
          code={`{
  "mcpServers": {
    "gestalt": {
      "url": "${origin}/mcp",
      "headers": {
        "Authorization": "Bearer gst_api_your_token_here"
      }
    }
  }
}`}
        />
      </section>
    </div>
  );
}

function useDeploymentOrigin() {
  const [origin, setOrigin] = useState(FALLBACK_ORIGIN);

  useEffect(() => {
    setOrigin(window.location.origin);
  }, []);

  return origin;
}

function Subheading({ id, title }: { id?: string; title: string }) {
  return (
    <h2
      id={id}
      className="scroll-mt-24 pt-2 text-lg font-semibold tracking-[-0.01em] text-primary"
    >
      {title}
    </h2>
  );
}

function CodeBlock({ code }: { code: string }) {
  const [copied, setCopied] = useState(false);

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {}
  }

  return (
    <div className="group relative">
      <pre className="overflow-x-auto rounded-xl border border-alpha bg-base-100 px-4 py-4 pr-12 font-mono text-sm leading-6 text-primary dark:bg-surface">
        <code>{code}</code>
      </pre>
      <button
        onClick={handleCopy}
        className="absolute right-3 top-3 rounded-md p-1.5 text-muted opacity-0 transition-all duration-150 hover:bg-alpha-5 hover:text-primary group-hover:opacity-100"
        title="Copy to clipboard"
        aria-label="Copy to clipboard"
      >
        {copied ? (
          <CheckIcon className="h-4 w-4 text-grove-600 dark:text-grove-200" />
        ) : (
          <CopyIcon className="h-4 w-4" />
        )}
      </button>
    </div>
  );
}

function InfoTable({ rows }: { rows: [string, string][] }) {
  return (
    <div className="overflow-hidden rounded-xl border border-alpha">
      <table className="w-full border-collapse bg-base-white text-left text-sm dark:bg-surface">
        <tbody>
          {rows.map(([label, value]) => (
            <tr key={label} className="border-t border-alpha first:border-t-0">
              <th className="w-56 bg-base-100 px-4 py-3 align-top font-medium text-primary dark:bg-surface-raised">
                {label}
              </th>
              <td className="px-4 py-3 text-muted">{value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
