
import { Link } from "@tanstack/react-router";
import { useEffect, useState, type ReactNode } from "react";
import { CodeBlock } from "@/components/ui/code-block";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderTitle,
} from "@/components/ui/section-header";

const FALLBACK_ORIGIN = "https://your-gestalt-host";

const mcpTabs = [
  { id: "mcp-claude-code", label: "Claude Code" },
  { id: "mcp-codex", label: "Codex" },
  { id: "mcp-cursor", label: "Cursor" },
  { id: "mcp-other", label: "Other Clients" },
] as const;

const agentEnvironmentTabs = [
  { id: "agent-claude-code", label: "Claude Code web" },
  { id: "agent-codex", label: "Codex Cloud" },
  { id: "agent-cursor", label: "Cursor Cloud Agents" },
] as const;

type McpTabId = (typeof mcpTabs)[number]["id"];
type AgentEnvironmentTabId = (typeof agentEnvironmentTabs)[number]["id"];

const mcpTabIds = mcpTabs.map((tab) => tab.id);
const defaultMcpTabId: McpTabId = "mcp-claude-code";

const agentEnvironmentTabIds = agentEnvironmentTabs.map((tab) => tab.id);
const defaultAgentEnvironmentTabId: AgentEnvironmentTabId = "agent-claude-code";

function agentStartupScript() {
  return "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh";
}

function cloudEnvironmentVariables(origin: string) {
  return `GESTALT_URL=${origin}
GESTALT_API_KEY=gst_api_your_token_here`;
}

export function GettingStartedDocsPage() {
  const origin = useDeploymentOrigin();

  return (
    <>
      <DocsPageHeader
        title="Getting Started"
        description={
          <>
            This guide covers the user-facing workflows for the Gestalt
            workspace you are currently using: install{" "}
            <InlineCode>gestalt</InlineCode>,
            point it at this workspace, sign in when required, connect
            apps, grant authorization, invoke operations, mint API tokens,
            and attach an MCP-aware client. No command-line experience is
            required. Follow the pages below and copy the commands as-is.
          </>
        }
      />
      <DocsPageBody>
        <div className="rounded-xl border border-border bg-card p-5 text-card-foreground">
          <p className="text-xs font-medium uppercase tracking-[0.16em] text-muted-foreground/70">
            Base URL
          </p>
          <p className="mt-2 font-mono text-sm text-foreground">{origin}</p>
        </div>
        <Subheading id="install" title="Install" />
        <p className="doc-copy">
          End users only need the{" "}
          <InlineCode>gestalt</InlineCode> CLI.
        </p>
        <p className="doc-copy">
          The recommended way to install is the Gestalt installer script.
        </p>
        <CodeBlock language="cli" code="curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh" />
        <p className="doc-copy">
          If you prefer a package manager, use Homebrew. Manual archives are
          also available on the{" "}
          <a
            href="https://github.com/valon-technologies/gestalt/releases"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            GitHub releases page
          </a>
          .
        </p>
        <CodeBlock
          language="cli"
          code={`brew tap valon-technologies/gestalt
brew install valon-technologies/gestalt/gestalt`}
        />
        <p className="doc-copy">
          Then verify the CLI is on your{" "}
          <InlineCode>PATH</InlineCode>.
        </p>
        <CodeBlock language="cli" code="gestalt --version" />

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
          <InlineCode>.gestalt/config.json</InlineCode>{" "}
          file stores only the base URL. The CLI searches the current directory
          and then parent directories until it finds the nearest project
          config.
        </p>
        <div className="doc-copy space-y-2">
          <p>Resolution order:</p>
          <ol className="list-decimal space-y-1 pl-6">
            <li>
              <InlineCode>--url</InlineCode>
            </li>
            <li>
              <InlineCode>GESTALT_URL</InlineCode>
            </li>
            <li>
              project-local{" "}
              <InlineCode>.gestalt/config.json</InlineCode>
            </li>
            <li>
              user-local CLI config file, for example{" "}
              <InlineCode>~/.config/gestalt/config.json</InlineCode>
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
        <CodeBlock language="cli" code="gestalt apps list" />

        <Subheading id="authorization" title="Grant authorization" />
        <p className="doc-copy">
          Use authorization grants when another user or service account needs
          access to an app. App admins can manage members for their own app;
          built-in Gestalt admins can manage grants across apps.
        </p>
        <CodeBlock
          language="cli"
          code={`gestalt authorization apps members set <app> \\
  --email operator@example.com \\
  --role viewer

gestalt authorization subjects grants set service_account:release-bot <app> \\
  --role viewer`}
        />
        <p className="doc-copy">
          For service account setup, built-in admin grants, and split
          management listener deployments, open{" "}
          <Link to="/docs/authorization" className="doc-link">
            Grant Authorization
          </Link>
          .
        </p>

        <Subheading
          id="agent-environments"
          title="Configure cloud environments"
        />
        <p className="doc-copy">
          Configure the hosted coding environment before starting cloud tasks.
          Set the workspace URL and API token in that environment, then install
          the CLI in the platform setup or startup script.
        </p>
        <AgentEnvironmentTabs origin={origin} />

        <Subheading id="workflows" title="Inspect workflows" />
        <p className="doc-copy">
          After your workspace URL and auth are set, use{" "}
          <InlineCode>gestalt workflows</InlineCode>{" "}
          to inspect recent workflow runs from the CLI.
        </p>
        <CodeBlock
          language="cli"
          code={`gestalt workflows --help
gestalt workflows runs list`}
        />
        <p className="doc-copy">
          For a deeper walkthrough, open{" "}
          <Link to="/docs/workflows" className="doc-link">
            Workflows
          </Link>
          . If you prefer the browser, open an app&apos;s admin page and use the{" "}
          <strong>Workflows</strong> tab to inspect runs for that app.
        </p>
      </DocsPageBody>
    </>
  );
}

export function ConnectDocsPage() {
  return (
    <>
      <DocsPageHeader
        title="Connect Apps"
        description="Inspect available apps first, then connect the ones you need."
      />
      <DocsPageBody>
        <p className="doc-copy">
          Apps available in this workspace appear in both the CLI and the
          UI. Use either surface to start the underlying OAuth or manual
          credential flow.
        </p>
        <CodeBlock
          language="cli"
          code={`gestalt apps list
gestalt apps connect <app>
gestalt apps connect <app> --connection <name> --instance <instance>`}
        />
        <p className="doc-copy">
          If you prefer the browser flow, the same work is available on{" "}
          <Link to="/apps" className="doc-link">
            Apps
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
        title="Invoke Operations"
        description="Use the catalog built into Gestalt to discover an app's operations before making requests."
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
        title="Manage API Tokens"
        description="User tokens work for both the HTTP API and the MCP endpoint."
      />
      <DocsPageBody>
        <CodeBlock
          language="cli"
          code={`gestalt tokens create --name automation
gestalt tokens list
gestalt tokens revoke <token-id>`}
        />
        <p className="doc-copy">
          Tokens can also be created from{" "}
          <Link to="/authorization" className="doc-link">
            Authorization
          </Link>
          . The raw token value is shown once, so store it immediately in your
          secret manager or shell environment.
        </p>
      </DocsPageBody>
    </>
  );
}

export function AuthorizationDocsPage() {
  return (
    <>
      <DocsPageHeader
        title="Grant Authorization"
        description="Grant users and service accounts access to app operations from the Gestalt CLI."
      />
      <DocsPageBody>
        <p className="doc-copy">
          Most teams grant access at the app level. App admins can manage
          members for apps they administer. Built-in Gestalt admins can
          manage every app and the global admin set. If your deployment
          splits public and management listeners, pass{" "}
          <InlineCode>--url &lt;management-url&gt;</InlineCode>{" "}
          to admin authorization commands.
        </p>

        <Subheading id="authz-plugin-access" title="Grant app access" />
        <p className="doc-copy">
          Grant a user or service account an app role with{" "}
          <InlineCode>viewer</InlineCode>
          ,{" "}
          <InlineCode>editor</InlineCode>
          , or{" "}
          <InlineCode>admin</InlineCode>.
        </p>
        <CodeBlock
          language="cli"
          code={`gestalt authorization apps list
gestalt authorization apps members list <app>

gestalt authorization apps members set <app> \\
  --email operator@example.com \\
  --role viewer

gestalt authorization apps members set <app> \\
  --subject-id service_account:release-bot \\
  --role editor

gestalt authorization apps members remove <app> user:user_123`}
        />

        <Subheading id="authz-service-accounts" title="Grant service account access" />
        <p className="doc-copy">
          Service accounts are managed subjects. Create the subject, grant it a
          app role, connect any app credentials it needs, then mint a
          scoped token for automation.
        </p>
        <CodeBlock
          language="cli"
          code={`gestalt authorization subjects create release-bot \\
  --display-name "Release Bot"

gestalt authorization subjects grants set service_account:release-bot <app> \\
  --role viewer

gestalt authorization subjects integrations connect service_account:release-bot <app>

gestalt authorization subjects tokens create service_account:release-bot \\
  --name release-bot \\
  --permission <app>:<operation>`}
        />

        <Subheading id="authz-admins" title="Grant built-in admin access" />
        <p className="doc-copy">
          Built-in admins can administer the global authorization surface. Use
          this only for operators who should manage grants beyond one app.
        </p>
        <CodeBlock
          language="cli"
          code={`gestalt authorization admins members list

gestalt authorization admins members set \\
  --email admin@example.com \\
  --role admin

gestalt authorization admins members remove user:user_123`}
        />

        <Subheading id="authz-inspect" title="Inspect grants" />
        <p className="doc-copy">
          Use provider and relationship views to confirm which authorization
          provider is active and which dynamic app grants are stored.
        </p>
        <CodeBlock
          language="cli"
          code={`gestalt authorization provider get
gestalt authorization models list
gestalt authorization relationships list \\
  --resource-type plugin_dynamic \\
  --resource-id <app>`}
        />
      </DocsPageBody>
    </>
  );
}

export function WorkflowsDocsPage() {
  return (
    <>
      <DocsPageHeader
        eyebrow="Workflows"
        title="Inspect Workflows"
        description="Use the workflow CLI to inspect durable workflow run history without leaving the terminal."
      />
      <DocsPageBody>
        <p className="doc-copy">
          Start by checking the commands exposed by the CLI installed on your machine.
          Different builds may expose different workflow subcommands, so{" "}
          <InlineCode>--help</InlineCode> is the
          fastest source of truth.
        </p>

        <Subheading id="wf-help" title="Start with help" />
        <CodeBlock language="cli" code="gestalt workflows --help" />
        <p className="doc-copy">
          In this workspace, the default browser UI focuses on recent workflow
          execution history and durable per-step state.
        </p>

        <Subheading id="wf-runs" title="Inspect runs" />
        <p className="doc-copy">
          Run history tells you whether work executed, which definition and
          generation were used, which step is current, and which inputs and
          outputs were captured.
        </p>
        <CodeBlock
          language="cli"
          code={`gestalt workflows runs list
gestalt workflows runs list --app <app>
gestalt workflows runs get <run-id>`}
        />
        <p className="doc-copy">
          In the browser, open an app&apos;s admin page at{" "}
          <InlineCode>/apps/&lt;app&gt;/admin/workflows</InlineCode>{" "}
          for run history,{" "}
          <InlineCode>/apps/&lt;app&gt;/admin/workflows/runs/&lt;run-id&gt;</InlineCode>{" "}
          for a single run, and{" "}
          <InlineCode>/apps/&lt;app&gt;/admin/workflows/definitions</InlineCode>{" "}
          for definition inventory.
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
        title="Use With MCP"
        description="Gestalt exposes a single MCP endpoint that gives AI tools access to all your connected apps. If authentication is enabled, create an API token on the API Tokens page first."
      />
      <DocsPageBody>
        <p className="doc-copy">
          On workspaces with authentication disabled, omit the bearer-token flag
          and header blocks shown below.
        </p>
        <p className="doc-copy">
          These examples assume the agent environment runs this startup script
          before the MCP client starts.
        </p>
        <CodeBlock language="cli" code={agentStartupScript()} />
        <InfoTable
          rows={[
            ["Endpoint", `${origin}/mcp`],
            [
              "Authentication",
              "Authorization: Bearer gst_api_... when auth is enabled",
            ],
            [
              "If no tools appear",
              "Confirm that the app is MCP-enabled and connected for your user.",
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
          <InlineCode>gestalt auth login</InlineCode>
          , or set{" "}
          <InlineCode>GESTALT_API_KEY</InlineCode>{" "}
          if you are using a token directly.
        </p>

        <Subheading
          id="ts-multiple-connections"
          title="An app has multiple connections"
        />
        <p className="doc-copy">
          Pass{" "}
          <InlineCode>--connection</InlineCode>{" "}
          or{" "}
          <InlineCode>--instance</InlineCode> so
          Gestalt can resolve the correct credentials.
        </p>

        <Subheading
          id="ts-empty-tools"
          title="The MCP endpoint is mounted, but the tool list is empty"
        />
        <p className="doc-copy">
          That usually means the app is available in the workspace config
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
  eyebrow?: string;
  title: string;
  description: ReactNode;
}) {
  const showEyebrow = eyebrow != null && eyebrow !== title;

  return (
    <PageHeader className="scroll-mt-24 border-b border-alpha pb-10">
      <PageHeaderContent size="lg">
        {showEyebrow ? <Eyebrow>{eyebrow}</Eyebrow> : null}
        <PageHeaderTitle>{title}</PageHeaderTitle>
        <PageHeaderDescription className="max-w-3xl text-foreground/80">
          {description}
        </PageHeaderDescription>
      </PageHeaderContent>
    </PageHeader>
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
    <div>
      <div
        role="tablist"
        aria-label="CLI setup methods"
        className="flex flex-wrap gap-5 border-b border-border"
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
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:border-ring hover:text-foreground"
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
                ? "space-y-4 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
                : "hidden"
            }
          >
            <CodeBlock language="cli" code={item.code} />
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
    <div>
      <div
        role="tablist"
        aria-label="Authentication methods"
        className="flex flex-wrap gap-5 border-b border-border"
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
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:border-ring hover:text-foreground"
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
                ? "space-y-4 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
                : "hidden"
            }
          >
            <CodeBlock language="cli" code={item.code} />
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
    <div>
      <div
        role="tablist"
        aria-label="Invocation methods"
        className="flex flex-wrap gap-5 border-b border-border"
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
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:border-ring hover:text-foreground"
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
            ? "space-y-4 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
            : "hidden"
        }
      >
        <CodeBlock
          language="cli"
          code={`gestalt apps invoke <app>
gestalt apps describe <app> <operation>
gestalt apps invoke <app> <operation> -p key=value
gestalt apps invoke <app> <operation> -p filters:='{"status":"open"}'
gestalt apps invoke <app> <operation> --input-file payload.json --select data.items`}
        />
        <p className="doc-copy">
          If you omit the operation,{" "}
          <InlineCode>gestalt apps invoke &lt;app&gt;</InlineCode>{" "}
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
            ? "space-y-4 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
            : "hidden"
        }
      >
        <p className="doc-copy">
          The CLI calls the same HTTP API that the workspace exposes for direct
          programmatic access. Use the app catalog route for discovery and the
          app-specific invoke route for operation calls.
        </p>
        <CodeBlock
          language="cli"
          code={`curl \\
  -H "Authorization: Bearer $GESTALT_API_KEY" \\
  ${origin}/api/v1/apps

curl \\
  -H "Authorization: Bearer $GESTALT_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"example":"value"}' \\
  ${origin}/api/v1/<app>/<operation>`}
        />
      </section>
    </div>
  );
}

function AgentEnvironmentTabs({ origin }: { origin: string }) {
  const [activeTabId, setActiveTabId] = useHashTab(
    agentEnvironmentTabIds,
    defaultAgentEnvironmentTabId,
  );

  return (
    <div>
      <div
        role="tablist"
        aria-label="Cloud environment configuration"
        className="flex flex-wrap gap-5 border-b border-border"
      >
        {agentEnvironmentTabs.map((tab) => {
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
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:border-ring hover:text-foreground"
              }`}
            >
              {tab.label}
            </button>
          );
        })}
      </div>

      <section
        id="agent-codex-panel"
        role="tabpanel"
        aria-labelledby="agent-codex"
        hidden={activeTabId !== "agent-codex"}
        className={
          activeTabId === "agent-codex"
            ? "space-y-5 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Navigate to{" "}
          <a
            href="https://chatgpt.com/codex/settings/environments"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            Codex environment settings
          </a>
          , open the cloud environment, and add these environment variables.
          Use a scoped API token for the cloud agent.
        </p>
        <CodeBlock language="cli" code={cloudEnvironmentVariables(origin)} />
        <p className="doc-copy">
          Then add this to the environment setup script.
        </p>
        <CodeBlock language="cli" code={agentStartupScript()} />
        <p className="doc-copy">
          Keep the values above in the cloud environment variables, not in the
          setup script. Codex secrets are only available during setup.
        </p>
        <p className="doc-copy">
          Reference:{" "}
          <a
            href="https://developers.openai.com/codex/cloud/environments"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            Codex cloud environments
          </a>
          .
        </p>
      </section>

      <section
        id="agent-cursor-panel"
        role="tabpanel"
        aria-labelledby="agent-cursor"
        hidden={activeTabId !== "agent-cursor"}
        className={
          activeTabId === "agent-cursor"
            ? "space-y-5 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Navigate to{" "}
          <a
            href="https://cursor.com/dashboard/cloud-agents#environments"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            Cursor Cloud Agents settings
          </a>
          , configure the workspace URL as an environment variable, and add the
          API token as a Cursor secret. Put the install command in{" "}
          <InlineCode>.cursor/environment.json</InlineCode>
          .
        </p>
        <CodeBlock
          language="json"
          code={`{
  "install": "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh"
}`}
        />
        <p className="doc-copy">
          Set{" "}
          <InlineCode>GESTALT_URL</InlineCode>{" "}
          to{" "}
          <InlineCode>{origin}</InlineCode> and{" "}
          <InlineCode>GESTALT_API_KEY</InlineCode>{" "}
          as a Cursor Cloud Agent secret containing a Gestalt API token. Cursor
          provides the secret to the agent environment at runtime under that
          variable name.
        </p>
        <p className="doc-copy">
          Reference:{" "}
          <a
            href="https://cursor.com/docs/cloud-agent"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            Cursor Cloud Agents
          </a>
          .
        </p>
      </section>

      <section
        id="agent-claude-code-panel"
        role="tabpanel"
        aria-labelledby="agent-claude-code"
        hidden={activeTabId !== "agent-claude-code"}
        className={
          activeTabId === "agent-claude-code"
            ? "space-y-5 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Navigate to{" "}
          <a
            href="https://claude.ai/code"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            claude.ai/code
          </a>
          , choose the cloud environment, and open its settings.
        </p>
        <img
          src="/docs/claude-code-web-environment.png"
          alt="Claude Code web environment picker with the settings control highlighted"
          width={1170}
          height={558}
          className="w-full rounded-lg border border-border"
        />
        <p className="doc-copy">
          Add environment variables in the cloud environment editor. Values use{" "}
          <InlineCode>.env</InlineCode> format.
        </p>
        <CodeBlock language="cli" code={cloudEnvironmentVariables(origin)} />
        <p className="doc-copy">
          Then add this to the cloud environment setup script.
        </p>
        <CodeBlock language="cli" code={agentStartupScript()} />
        <p className="doc-copy">
          Reference:{" "}
          <a
            href="https://code.claude.com/docs/en/claude-code-on-the-web"
            target="_blank"
            rel="noreferrer"
            className="doc-link"
          >
            Claude Code web
          </a>
          .
        </p>
      </section>
    </div>
  );
}

function McpClientTabs({ origin }: { origin: string }) {
  const [activeTabId, setActiveTabId] = useHashTab(mcpTabIds, defaultMcpTabId);

  return (
    <div>
      <div
        role="tablist"
        aria-label="MCP client configuration"
        className="flex flex-wrap gap-5 border-b border-border"
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
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:border-ring hover:text-foreground"
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
            ? "space-y-5 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Use{" "}
          <InlineCode>.mcp.json</InlineCode>{" "}
          for a project-scoped workspace shared in version control, or{" "}
          <InlineCode>~/.claude.json</InlineCode>{" "}
          for a private local or user-scoped config.
        </p>
        <CodeBlock
          language="json"
          code={`{
  "mcpServers": {
    "gestalt": {
      "type": "http",
      "url": "\${GESTALT_URL}/mcp",
      "headers": {
        "Authorization": "Bearer \${GESTALT_API_KEY}"
      }
    }
  }
}`}
        />
        <p className="doc-copy">Or add it from the CLI:</p>
        <CodeBlock
          language="cli"
          code={`claude mcp add --transport http --scope project \\
  --header "Authorization: Bearer $GESTALT_API_KEY" \\
  gestalt "$GESTALT_URL/mcp"`}
        />
      </section>

      <section
        id="mcp-codex-panel"
        role="tabpanel"
        aria-labelledby="mcp-codex"
        hidden={activeTabId !== "mcp-codex"}
        className={
          activeTabId === "mcp-codex"
            ? "space-y-5 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Codex can register the workspace directly from the CLI:
        </p>
        <CodeBlock
          language="cli"
          code={`codex mcp add gestalt --url "$GESTALT_URL/mcp" --bearer-token-env-var GESTALT_API_KEY`}
        />
        <p className="doc-copy">
          If authentication is disabled, omit{" "}
          <InlineCode>--bearer-token-env-var GESTALT_API_KEY</InlineCode>{" "}
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
            ? "space-y-5 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
            : "hidden"
        }
      >
        <p className="doc-copy">
          Config file:{" "}
          <InlineCode>.cursor/mcp.json</InlineCode>{" "}
          in your project root, or{" "}
          <InlineCode>~/.cursor/mcp.json</InlineCode>{" "}
          globally.
        </p>
        <CodeBlock
          language="json"
          code={`{
  "mcpServers": {
    "gestalt": {
      "url": "\${env:GESTALT_URL}/mcp",
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
            ? "space-y-5 rounded-b-xl border-x border-b border-border bg-card px-5 pb-5 pt-0 text-card-foreground [&:has(>p:first-child)]:pt-5 [&:has(>div:first-child)]:pt-5"
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
          language="json"
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

function InlineCode({ children }: { children: React.ReactNode }) {
  return (
    <code className="rounded-sm border border-border bg-muted px-[0.3em] py-[0.1em] font-mono text-[0.875em] text-foreground">
      {children}
    </code>
  );
}

function Subheading({ id, title }: { id?: string; title: string }) {
  return (
    <SectionHeader className="scroll-mt-24 pt-2">
      <SectionHeaderContent size="sm">
        <SectionHeaderTitle id={id}>{title}</SectionHeaderTitle>
      </SectionHeaderContent>
    </SectionHeader>
  );
}

function InfoTable({ rows }: { rows: [string, string][] }) {
  return (
    <div className="overflow-hidden rounded-xl border border-border">
      <table className="w-full border-collapse bg-card text-left text-sm text-card-foreground">
        <tbody>
          {rows.map(([label, value]) => (
            <tr key={label} className="border-t border-border first:border-t-0">
              <th className="w-56 bg-muted px-4 py-3 align-top font-medium text-foreground">
                {label}
              </th>
              <td className="px-4 py-3 text-muted-foreground">{value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
