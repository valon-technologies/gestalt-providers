
import { useEffect, useId, useState, type CSSProperties, type ReactNode } from "react";
import { Info } from "lucide-react";
import { CodeBlock } from "@/components/ui/code-block";
import { Code } from "@/components/ui/code";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SegmentedControl,
  type SegmentedControlOption,
} from "@/components/ui/segmented-control";
import {
  DOCS_AUTHORIZATION_PATH,
  DOCS_SETTINGS_TOKENS_HREF,
  DOCS_TOKENS_PATH,
  DOCS_WORKFLOWS_PATH,
} from "./docs-data";
import { DOCS_PAGE_TOP_GAP } from "./docs-chrome";
import { DocsLink } from "./DocsLink";

const FALLBACK_ORIGIN = "https://your-gestalt-host";

const mcpTabs = [
  { id: "mcp-claude-code", label: "Claude Code" },
  { id: "mcp-codex", label: "Codex" },
  { id: "mcp-cursor", label: "Cursor" },
  { id: "mcp-other", label: "Other clients" },
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

const GESTALT_INSTALL_SCRIPT =
  "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh";

const GESTALT_HOMEBREW_INSTALL = `brew tap valon-technologies/gestalt
brew install valon-technologies/gestalt/gestalt`;

/** Distribution methods for installing the gestalt CLI (not the CLI itself). */
const GESTALT_INSTALL_METHODS = [
  {
    id: "install-installer",
    label: "Installer",
    code: GESTALT_INSTALL_SCRIPT,
    description: "Recommended. Runs the Gestalt install script.",
  },
  {
    id: "install-homebrew",
    label: "Homebrew",
    code: GESTALT_HOMEBREW_INSTALL,
    description: "Use if you already manage tools with Homebrew.",
  },
] as const;

function agentStartupScript() {
  return GESTALT_INSTALL_SCRIPT;
}

function cloudEnvironmentVariables(origin: string) {
  return `GESTALT_URL=${origin}
GESTALT_API_KEY=gst_api_your_token_here`;
}

export function GettingStartedDocsPage() {
  const origin = useDeploymentOrigin();

  return (
    <>
      <DocsPageHeader title="Getting Started" />
      <DocsPageBody>
        <p>
          Walk through Gestalt setup with copy-paste{" "}
          <Code>gestalt</Code> CLI commands. You do not need prior CLI
          experience—run each command as shown. This page covers install, point
          the CLI, authenticate, then points you to connect apps, invoke
          operations, mint tokens, and MCP.
        </p>
        <div className="not-typeset flex flex-col gap-2.5">
          <Eyebrow className="block">Base URL</Eyebrow>
          <CodeBlock
            chrome="inset"
            language="plaintext"
            code={origin}
            copyLabel="Copy base URL"
          />
        </div>
        <Subheading id="install" title="Install" />
        <p>
          Install the <Code>gestalt</Code> CLI, then confirm it’s on your{" "}
          <Code>PATH</Code>.
        </p>
        <MethodCodeSwitcher
          label="Install methods"
          items={[...GESTALT_INSTALL_METHODS]}
        />
        <CodeBlock chrome="inset" language="cli" code="gestalt --version" />
        <p>
          Prefer a manual download? Get archives on the{" "}
          <DocsLink href="https://github.com/valon-technologies/gestalt/releases">
            GitHub releases page
          </DocsLink>
          .
        </p>

        <Subheading id="point-cli" title="Point the CLI at this workspace" />
        <p>
          The CLI needs the base URL for your Gestalt workspace. Use either the
          setup wizard or a direct config command.
        </p>
        <MethodCodeSwitcher
          label="CLI setup methods"
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
        <p>
          The optional{" "}
          <Code>.gestalt/config.json</Code>{" "}
          file stores only the base URL. The CLI searches the current directory
          and then parent directories until it finds the nearest project
          config.
        </p>
        <p>Resolution order:</p>
        <ol>
          <li>
            <Code>--url</Code>
          </li>
          <li>
            <Code>GESTALT_URL</Code>
          </li>
          <li>
            project-local <Code>.gestalt/config.json</Code>
          </li>
          <li>
            user-local CLI config file, for example{" "}
            <Code>~/.config/gestalt/config.json</Code>
          </li>
        </ol>

        <Subheading id="authenticate" title="Authenticate" />
        <p>
          Use browser login for interactive sessions, or set a token directly
          for scripts and other non-interactive clients. If authentication is
          disabled, you can skip both flows and call the API directly.
        </p>
        <MethodCodeSwitcher
          label="Authentication methods"
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
        <CodeBlock chrome="inset" language="cli" code="gestalt apps list" />

        <Subheading id="authorization" title="Grant App Access" />
        <p>
          App access for other users or service accounts is managed by app
          admins (and Gestalt admins). If you need access, ask your admin. If
          you grant access, see{" "}
          <DocsLink to={DOCS_AUTHORIZATION_PATH}>Grant App Access</DocsLink>{" "}
          for member, service-account, and admin commands.
        </p>
        <CodeBlock chrome="inset"
          language="cli"
          code={`gestalt authorization apps members set <app> \\
  --email operator@example.com \\
  --role viewer`}
        />

        <Subheading
          id="agent-environments"
          title="Configure cloud environments"
        />
        <p>
          Configure the hosted coding environment before starting cloud tasks.
          Set the workspace URL and API token in that environment, then install
          the CLI in the platform setup or startup script.
        </p>
        <AgentEnvironmentTabs origin={origin} />

        <Subheading id="workflows" title="Inspect Workflows" />
        <p>
          After your workspace URL and auth are set, inspect recent runs with{" "}
          <Code>gestalt workflows runs list</Code>. For CLI and
          browser paths, see{" "}
          <DocsLink to={DOCS_WORKFLOWS_PATH}>Inspect Workflows</DocsLink>
          .
        </p>
      </DocsPageBody>
    </>
  );
}

export function ConnectDocsPage() {
  return (
    <>
      <DocsPageHeader title="Connect Apps" />
      <DocsPageBody>
        <p>
          Run <Code>gestalt apps list</Code>, then{" "}
          <Code>gestalt apps connect &lt;app&gt;</Code>—or use the Apps page in
          the browser.
        </p>
        <p>
          Apps available in this workspace appear in both the CLI and the
          UI. Use either surface to start the underlying OAuth or manual
          credential flow.
        </p>
        <CodeBlock chrome="inset"
          language="cli"
          code={`gestalt apps list
gestalt apps connect <app>
gestalt apps connect <app> --connection <name> --instance <instance>`}
        />
        <p>
          If you prefer the browser flow, the same work is available on{" "}
          <DocsLink to="/apps">Apps</DocsLink>
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
      <DocsPageHeader title="Invoke Operations" />
      <DocsPageBody>
        <p>
          Use the catalog built into Gestalt to discover an app&apos;s
          operations before making requests.
        </p>
        <InvokeMethodTabs origin={origin} />
      </DocsPageBody>
    </>
  );
}

export function TokensDocsPage() {
  return (
    <>
      <DocsPageHeader title="Manage API Tokens" />
      <DocsPageBody>
        <p>
          User tokens work for both the HTTP API and the MCP endpoint.
        </p>
        <CodeBlock chrome="inset"
          language="cli"
          code={`gestalt tokens create --name automation
gestalt tokens list
gestalt tokens revoke <token-id>`}
        />
        <p>
          Tokens can also be created in the UI under{" "}
          <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>Settings → API tokens</DocsLink>
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
      <DocsPageHeader title="Grant App Access" />
      <DocsPageBody>
        <p>
          For app admins and Gestalt admins: grant users and service accounts
          access to app operations from the Gestalt CLI. End users cannot
          self-serve grants—ask your workspace admin.
        </p>
        <div className="not-typeset">
          <Alert variant="info">
            <Info aria-hidden="true" />
            <AlertTitle>Admin audience</AlertTitle>
            <AlertDescription>
              These commands manage who can invoke apps. Personal API tokens for
              scripts and MCP live under{" "}
              <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>
                Settings → API tokens
              </DocsLink>
              , not on this page.
            </AlertDescription>
          </Alert>
        </div>
        <p>
          Most teams grant access at the app level. App admins can manage
          members for apps they administer. Built-in Gestalt admins can
          manage every app and the global admin set. If your deployment
          splits public and management listeners, pass{" "}
          <Code>--url &lt;management-url&gt;</Code>{" "}
          to admin authorization commands.
        </p>

        <Subheading id="authz-plugin-access" title="Grant app access" />
        <p>
          Grant a user or service account an app role with{" "}
          <Code>viewer</Code>
          ,{" "}
          <Code>editor</Code>
          , or{" "}
          <Code>admin</Code>.
        </p>
        <CodeBlock chrome="inset"
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
        <p>
          Service accounts are managed subjects. Create the subject, grant it an
          app role, connect the app credentials it needs (same credential flow as{" "}
          <Code>gestalt apps connect</Code>, scoped to that
          subject), then mint a scoped token for automation.
        </p>
        <CodeBlock chrome="inset"
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
        <p>
          Built-in admins can administer the global authorization surface. Use
          this only for operators who should manage grants beyond one app.
        </p>
        <CodeBlock chrome="inset"
          language="cli"
          code={`gestalt authorization admins members list

gestalt authorization admins members set \\
  --email admin@example.com \\
  --role admin

gestalt authorization admins members remove user:user_123`}
        />

        <Subheading id="authz-inspect" title="Inspect grants" />
        <p>
          Use provider and relationship views to confirm which authorization
          provider is active and which app grants are stored.{" "}
          <Code>plugin_dynamic</Code> is the internal resource type
          for app-scoped grants.
        </p>
        <CodeBlock chrome="inset"
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
      <DocsPageHeader title="Inspect Workflows" />
      <DocsPageBody>
        <p>
          Use the workflow CLI to inspect durable workflow run history without
          leaving the terminal.
        </p>
        <p>
          Start by checking the commands exposed by the CLI installed on your machine.
          Different builds may expose different workflow subcommands, so{" "}
          <Code>--help</Code> is the
          fastest source of truth.
        </p>

        <Subheading id="wf-help" title="Start with help" />
        <CodeBlock chrome="inset" language="cli" code="gestalt workflows --help" />
        <p>
          In this workspace, the default browser UI focuses on recent workflow
          execution history and durable per-step state.
        </p>

        <Subheading id="wf-runs" title="Inspect runs" />
        <p>
          Run history tells you whether work executed, which definition and
          generation were used, which step is current, and which inputs and
          outputs were captured.
        </p>
        <CodeBlock chrome="inset"
          language="cli"
          code={`gestalt workflows runs list
gestalt workflows runs list --app <app>
gestalt workflows runs get <run-id>`}
        />
        <p>
          In the browser, open an app&apos;s admin page at{" "}
          <Code>/apps/&lt;app&gt;/admin/workflows</Code>{" "}
          for run history,{" "}
          <Code>/apps/&lt;app&gt;/admin/workflows/runs/&lt;run-id&gt;</Code>{" "}
          for a single run, and{" "}
          <Code>/apps/&lt;app&gt;/admin/workflows/definitions</Code>{" "}
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
      <DocsPageHeader title="Use With MCP" />
      <DocsPageBody>
        <p>
          Gestalt exposes a single MCP endpoint that gives AI tools access to
          all your connected apps. If authentication is enabled, create an API
          token first—in the UI at{" "}
          <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>Settings → API tokens</DocsLink>
          , or with <Code>gestalt tokens create</Code> (see{" "}
          <DocsLink to={DOCS_TOKENS_PATH}>Manage API Tokens</DocsLink>
          ).
        </p>
        <p>
          On workspaces with authentication disabled, omit the bearer-token flag
          and header blocks shown below.
        </p>
        <p>
          These examples assume the agent environment runs this startup script
          before the MCP client starts.
        </p>
        <CodeBlock chrome="inset" language="cli" code={agentStartupScript()} />
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
      <DocsPageHeader title="Troubleshooting" />
      <DocsPageBody>
        <p>
          Most user-facing problems come down to the wrong URL, expired auth,
          ambiguous connection selection, or a grant that has not taken effect
          yet.
        </p>
        <Subheading
          id="ts-not-authenticated"
          title="The CLI says you are not authenticated"
        />
        <p>
          Run{" "}
          <Code>gestalt auth login</Code>
          , or set{" "}
          <Code>GESTALT_API_KEY</Code>{" "}
          if you are using a token directly.
        </p>

        <Subheading
          id="ts-multiple-connections"
          title="An app has multiple connections"
        />
        <p>
          Pass{" "}
          <Code>--connection</Code>{" "}
          or{" "}
          <Code>--instance</Code> so
          Gestalt can resolve the correct credentials.
        </p>

        <Subheading
          id="ts-empty-tools"
          title="The MCP endpoint is mounted, but the tool list is empty"
        />
        <p>
          That usually means the app is available in the workspace config
          but has not been connected for your current user yet.
        </p>

        <Subheading
          id="ts-forbidden"
          title="I was granted access but still get forbidden"
        />
        <p>
          Confirm an app admin granted you the expected role (
          <DocsLink to={DOCS_AUTHORIZATION_PATH}>Grant App Access</DocsLink>
          ), that the operation allows that role, and that you are
          authenticated as the same subject the grant targets. Then retry the
          invoke or MCP call.
        </p>
      </DocsPageBody>
    </>
  );
}

function DocsPageHeader({
  eyebrow,
  title,
}: {
  eyebrow?: string;
  title: string;
}) {
  const showEyebrow = eyebrow != null && eyebrow !== title;

  return (
    <PageHeader className="scroll-mt-[var(--page-layout-anchor-offset)]">
      <PageHeaderContent size="lg">
        {showEyebrow ? <Eyebrow>{eyebrow}</Eyebrow> : null}
        <PageHeaderTitle>{title}</PageHeaderTitle>
      </PageHeaderContent>
    </PageHeader>
  );
}

function DocsPageBody({ children }: { children: ReactNode }) {
  // Registry reading contract: prose uses valon-typeset (lists, markers, code).
  // Chrome islands use `.not-typeset` or `[data-typeset-chrome]` (flow gap only).
  // Set h2 start on this node — `.typeset` owns the token and would ignore an
  // inherited value from PageLayout.
  return (
    <div
      className="typeset typeset-docs mt-[length:var(--typeset-flow,1.5em)]"
      style={
        {
          "--typeset-h2-margin-start": DOCS_PAGE_TOP_GAP,
        } as CSSProperties
      }
    >
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

function DocsOptionSwitcher<V extends string>({
  label,
  options,
  value,
  onValueChange,
  children,
}: {
  label: string;
  options: ReadonlyArray<SegmentedControlOption<V>>;
  value: V;
  onValueChange: (value: V) => void;
  children: ReactNode;
}) {
  // Stable panel id — never the option value. Hash-backed switchers write
  // `#${value}` for shareable selection; a matching DOM id would scroll the
  // panel under sticky app chrome.
  const panelId = useId();
  const activeLabel =
    options.find((option) => option.value === value)?.label ?? label;

  return (
    <div data-typeset-chrome data-docs-option-switcher>
      {/*
        Horizontal scroll for long labelled tracks. `overflow-x-auto` forces
        y-clipping too (CSS overflow pairing), so pad the clip edges for outward
        focus rings.
      */}
      <div className="not-typeset -mx-1 -mt-1 min-w-0 overflow-x-auto px-1 pb-1 pt-1">
        <SegmentedControl
          size="sm"
          label={label}
          value={value}
          onValueChange={onValueChange}
          options={options}
          panelId={panelId}
          showLabels
        />
      </div>
      <div
        id={panelId}
        role="region"
        aria-label={`${label}: ${activeLabel}`}
        aria-live="polite"
      >
        {children}
      </div>
    </div>
  );
}

function MethodCodeSwitcher({
  label,
  items,
}: {
  label: string;
  items: { id: string; label: string; code: string; description: string }[];
}) {
  const [activeId, setActiveId] = useState(items[0]?.id ?? "");
  const active = items.find((item) => item.id === activeId) ?? items[0];
  if (!active) return null;

  return (
    <DocsOptionSwitcher
      label={label}
      options={items.map((item) => ({ value: item.id, label: item.label }))}
      value={activeId}
      onValueChange={setActiveId}
    >
      <p>{active.description}</p>
      <CodeBlock chrome="inset" language="cli" code={active.code} />
    </DocsOptionSwitcher>
  );
}

function InvokeMethodTabs({ origin }: { origin: string }) {
  const [activeId, setActiveId] = useState<"invoke-cli" | "invoke-http">(
    "invoke-cli",
  );

  return (
    <DocsOptionSwitcher
      label="Invocation methods"
      options={[
        { value: "invoke-cli", label: "CLI" },
        { value: "invoke-http", label: "HTTP" },
      ]}
      value={activeId}
      onValueChange={setActiveId}
    >
      {activeId === "invoke-cli" ? (
        <>
          <CodeBlock chrome="inset"
            language="cli"
            code={`gestalt apps invoke <app>
gestalt apps describe <app> <operation>
gestalt apps invoke <app> <operation> -p key=value
gestalt apps invoke <app> <operation> -p filters:='{"status":"open"}'
gestalt apps invoke <app> <operation> --input-file payload.json --select data.items`}
          />
          <p>
            If you omit the operation,{" "}
            <Code>gestalt apps invoke &lt;app&gt;</Code>{" "}
            lists available operations instead of running one.
          </p>
        </>
      ) : (
        <>
          <p>
            The CLI calls the same HTTP API that the workspace exposes for direct
            programmatic access. Use the app catalog route for discovery and the
            app-specific invoke route for operation calls.
          </p>
          <CodeBlock chrome="inset"
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
        </>
      )}
    </DocsOptionSwitcher>
  );
}

function AgentEnvironmentTabs({ origin }: { origin: string }) {
  const [activeTabId, setActiveTabId] = useHashTab(
    agentEnvironmentTabIds,
    defaultAgentEnvironmentTabId,
  );

  return (
    <DocsOptionSwitcher
      label="Cloud environment configuration"
      options={agentEnvironmentTabs.map((tab) => ({
        value: tab.id,
        label: tab.label,
      }))}
      value={activeTabId as AgentEnvironmentTabId}
      onValueChange={setActiveTabId}
    >
      {activeTabId === "agent-codex" ? (
        <>
          <p>
            Navigate to{" "}
            <DocsLink href="https://chatgpt.com/codex/settings/environments">Codex environment settings</DocsLink>
            , open the cloud environment, and add these environment variables.
            Use a scoped API token for the cloud agent.
          </p>
          <CodeBlock chrome="inset" language="cli" code={cloudEnvironmentVariables(origin)} />
          <p>
            Then add this to the environment setup script.
          </p>
          <CodeBlock chrome="inset" language="cli" code={agentStartupScript()} />
          <p>
            Keep the values above in the cloud environment variables, not in the
            setup script. Codex secrets are only available during setup.
          </p>
          <p>
            Reference:{" "}
            <DocsLink href="https://developers.openai.com/codex/cloud/environments">Codex cloud environments</DocsLink>
            .
          </p>
        </>
      ) : null}

      {activeTabId === "agent-cursor" ? (
        <>
          <p>
            Navigate to{" "}
            <DocsLink href="https://cursor.com/dashboard/cloud-agents#environments">Cursor Cloud Agents settings</DocsLink>
            , configure the workspace URL as an environment variable, and add the
            API token as a Cursor secret. Put the install command in{" "}
            <Code>.cursor/environment.json</Code>
            .
          </p>
          <CodeBlock
            language="json"
            filename=".cursor/environment.json"
            code={`{
  "install": "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh"
}`}
          />
          <p>
            Set{" "}
            <Code>GESTALT_URL</Code>{" "}
            to{" "}
            <Code>{origin}</Code> and{" "}
            <Code>GESTALT_API_KEY</Code>{" "}
            as a Cursor Cloud Agent secret containing a Gestalt API token. Cursor
            provides the secret to the agent environment at runtime under that
            variable name.
          </p>
          <p>
            Reference:{" "}
            <DocsLink href="https://cursor.com/docs/cloud-agent">Cursor Cloud Agents</DocsLink>
            .
          </p>
        </>
      ) : null}

      {activeTabId === "agent-claude-code" ? (
        <>
          <p>
            Navigate to{" "}
            <DocsLink href="https://claude.ai/code">claude.ai/code</DocsLink>
            , choose the cloud environment, and open its settings.
          </p>
          <img
            src="/docs/claude-code-web-environment.png"
            alt="Claude Code web environment picker with the settings control highlighted"
            width={1170}
            height={558}
            className="w-full rounded-lg border border-border"
          />
          <p>
            Add environment variables in the cloud environment editor. Values use{" "}
            <Code>.env</Code> format.
          </p>
          <CodeBlock chrome="inset" language="cli" code={cloudEnvironmentVariables(origin)} />
          <p>
            Then add this to the cloud environment setup script.
          </p>
          <CodeBlock chrome="inset" language="cli" code={agentStartupScript()} />
          <p>
            Reference:{" "}
            <DocsLink href="https://code.claude.com/docs/en/claude-code-on-the-web">Claude Code web</DocsLink>
            .
          </p>
        </>
      ) : null}
    </DocsOptionSwitcher>
  );
}

function McpClientTabs({ origin }: { origin: string }) {
  const [activeTabId, setActiveTabId] = useHashTab(mcpTabIds, defaultMcpTabId);

  return (
    <DocsOptionSwitcher
      label="MCP client configuration"
      options={mcpTabs.map((tab) => ({ value: tab.id, label: tab.label }))}
      value={activeTabId as McpTabId}
      onValueChange={setActiveTabId}
    >
      {activeTabId === "mcp-claude-code" ? (
        <>
          <p>
            Use{" "}
            <Code>.mcp.json</Code>{" "}
            for a project-scoped workspace shared in version control, or{" "}
            <Code>~/.claude.json</Code>{" "}
            for a private local or user-scoped config.
          </p>
          <CodeBlock
            language="json"
            filename=".mcp.json"
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
          <p>Or add it from the CLI:</p>
          <CodeBlock chrome="inset"
            language="cli"
            code={`claude mcp add --transport http --scope project \\
  --header "Authorization: Bearer $GESTALT_API_KEY" \\
  gestalt "$GESTALT_URL/mcp"`}
          />
        </>
      ) : null}

      {activeTabId === "mcp-codex" ? (
        <>
          <p>
            Codex can register the workspace directly from the CLI:
          </p>
          <CodeBlock chrome="inset"
            language="cli"
            code={`codex mcp add gestalt --url "$GESTALT_URL/mcp" --bearer-token-env-var GESTALT_API_KEY`}
          />
          <p>
            If authentication is disabled, omit{" "}
            <Code>--bearer-token-env-var GESTALT_API_KEY</Code>{" "}
            from the command.
          </p>
        </>
      ) : null}

      {activeTabId === "mcp-cursor" ? (
        <>
          <p>
            Config file:{" "}
            <Code>.cursor/mcp.json</Code>{" "}
            in your project root, or{" "}
            <Code>~/.cursor/mcp.json</Code>{" "}
            globally.
          </p>
          <CodeBlock
            language="json"
            filename=".cursor/mcp.json"
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
        </>
      ) : null}

      {activeTabId === "mcp-other" ? (
        <>
          <p>
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
          <CodeBlock chrome="inset"
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
        </>
      ) : null}
    </DocsOptionSwitcher>
  );
}

function useDeploymentOrigin() {
  const [origin, setOrigin] = useState(() =>
    typeof window === "undefined" ? FALLBACK_ORIGIN : window.location.origin,
  );

  useEffect(() => {
    setOrigin(window.location.origin);
  }, []);

  return origin;
}

function Subheading({ id, title }: { id?: string; title: string }) {
  // Real heading so Registry typeset owns section rhythm (h2 margins /
  // after-heading). `id` and `scroll-mt` must share the same node — hash /
  // scrollIntoView only apply scroll-margin on the matched element.
  // Offset token includes measured sticky chrome (worktree banner + top bar).
  return (
    <h2 id={id} className="scroll-mt-[var(--page-layout-anchor-offset)]">
      {title}
    </h2>
  );
}

function InfoTable({ rows }: { rows: [string, string][] }) {
  return (
    <div className="not-typeset overflow-hidden rounded-xl border border-border">
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
