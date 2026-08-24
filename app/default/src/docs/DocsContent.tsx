
import { useEffect, useState, type CSSProperties, type ReactNode } from "react";
import { Info, TriangleAlert } from "lucide-react";
import { AlertDescription, AlertTitle, Callout } from "@/components/ui/alert";
import { CodeBlock } from "@/components/ui/code-block";
import { Code } from "@/components/ui/code";
import { Link } from "@/components/ui/link";
import {
  DescriptionDetails,
  DescriptionItem,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  DOCS_AUTHORIZATION_PATH,
  DOCS_CLI_PATH,
  DOCS_CONNECT_PATH,
  DOCS_GETTING_STARTED_PATH,
  DOCS_INVOKE_PATH,
  DOCS_MCP_PATH,
  DOCS_SETTINGS_TOKENS_HREF,
  DOCS_TOKENS_PATH,
  DOCS_TROUBLESHOOTING_PATH,
  DOCS_WORKFLOWS_PATH,
  docsSubsectionLabel,
} from "./docs-data";
import { ASSISTANT_OVERLAP_TITLE } from "@/lib/assistantConnectionCopy";
import {
  assistantDocsLandingHash,
  assistantHostById,
} from "@/lib/assistantHosts";
import { SETUP_JOURNEY_LABEL } from "@/lib/setupJourneyCopy";
import { SETUP_PATH } from "@/lib/constants";
import { gestaltMcpClientConfigJson } from "@/lib/gestaltMcpClientConfig";
import { DOCS_TOKEN_PLACEHOLDER } from "./assistant-destinations";
import { resolveGestaltPublicOrigin } from "@/lib/gestaltPublicOrigin";
import { DocsAudienceCallout } from "./DocsAudienceCallout";
import { AssistantDestinationSwitcher } from "./AssistantDestinationSwitcher";
import { DocsOptionSwitcher, useHashTab } from "./docs-option-switcher";
import { DOCS_PAGE_TOP_GAP } from "./docs-chrome";
import { DocsLink } from "./DocsLink";

const agentEnvironmentTabs = [
  { id: "agent-claude-code", label: "Claude Code web" },
  { id: "agent-codex", label: "Codex Cloud" },
  { id: "agent-cursor", label: "Cursor Cloud Agents" },
] as const;

type AgentEnvironmentTabId = (typeof agentEnvironmentTabs)[number]["id"];

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

function cloudEnvironmentVariables(origin: string) {
  return `GESTALT_URL=${origin}
GESTALT_API_KEY=${DOCS_TOKEN_PLACEHOLDER}`;
}

function WorkspaceUrlBlock({ origin }: { origin: string }) {
  return (
    <>
      <div className="not-typeset flex flex-col gap-2.5">
        <Eyebrow className="block">Base URL</Eyebrow>
        <CodeBlock
          chrome="inset"
          language="plaintext"
          code={origin}
          copyLabel="Copy base URL"
        />
      </div>
      <p>
        Copy this URL when a client or the CLI asks for a Gestalt host.
      </p>
    </>
  );
}

export function GettingStartedDocsPage() {
  const origin = useDeploymentOrigin();

  return (
    <>
      <DocsPageHeader title="Getting Started" />
      <DocsPageBody>
        <p>
          Gestalt is an API proxy: a central hub for managing authentication
          across the tools you use at work. Instead of configuring each App
          separately (Slack, Notion, GitHub, and more), you authenticate once
          through Gestalt and it handles the rest.
        </p>
        <p>
          Think of it as a universal key for your tools. You log in once, and
          Gestalt securely manages your access behind the scenes.
        </p>
        <p>
          Choose the Apps you need, then create an API token for MCP clients
          and the CLI. This page covers those first steps in the browser only.
          Prefer a guided walkthrough? Open{"\u00a0"}
          <DocsLink to={SETUP_PATH}>{SETUP_JOURNEY_LABEL}</DocsLink>.
        </p>
        <WorkspaceUrlBlock origin={origin} />
        <div className="not-typeset">
          <Callout variant="info">
            <Info aria-hidden="true" />
            <AlertTitle>How access works</AlertTitle>
            <AlertDescription>
              Gestalt connects the Apps you can use to agents and automation.
              Access is limited to your identity and the permissions you have
              been granted. Gestalt does not provide blanket access to every
              App.
            </AlertDescription>
          </Callout>
        </div>

        <Subheading id="connect-apps" />
        <p>
          Open <DocsLink to="/apps">Apps</DocsLink>, choose an App, and complete
          its OAuth or manual credential flow. Confirm the App shows as
          connected for your account.
        </p>
        <p>
          To connect from the terminal, install the{" "}
          <DocsLink to={DOCS_CLI_PATH}>Gestalt CLI</DocsLink> first, then see{" "}
          <DocsLink to={DOCS_CONNECT_PATH}>Connect apps</DocsLink>.
        </p>

        <Subheading id="create-token" />
        <p>
          Open{" "}
          <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>
            Settings → API tokens
          </DocsLink>
          , click Create token, give it a descriptive name, and copy the secret
          immediately. Then open{" "}
          <DocsLink to={DOCS_MCP_PATH}>MCP Clients</DocsLink>, pick your
          assistant, and follow those steps.
        </p>
        <div className="not-typeset">
          <Callout variant="warning">
            <TriangleAlert aria-hidden="true" />
            <AlertTitle>This token is shown only once</AlertTitle>
            <AlertDescription>
              Save it in a password manager or secret store. Do not share it.
              Anyone with the token can act as you through the Apps you have
              connected.
            </AlertDescription>
          </Callout>
        </div>

        <Subheading id="next-steps" />
        <p>
          If you use Claude Code, ChatGPT, Codex, Cursor, or another assistant,
          open{" "}
          <DocsLink to={DOCS_MCP_PATH}>MCP Clients</DocsLink> next. That page
          shows how to save this workspace and your token in the assistant&apos;s
          settings. You do not need the Gestalt CLI.
        </p>
        <p>
          If you work in the terminal, install the CLI first. Then you can
          connect Apps and invoke operations from the shell.
        </p>
        <ul>
          <li>
            <DocsLink to={DOCS_CLI_PATH}>Gestalt CLI</DocsLink>
            : install the CLI, point it at this workspace, and authenticate.
          </li>
          <li>
            <DocsLink to={DOCS_CONNECT_PATH}>Connect apps</DocsLink>
            : connect from the terminal after the CLI is installed.
          </li>
          <li>
            <DocsLink to={DOCS_INVOKE_PATH}>Invoke Operations</DocsLink>
            : discover and run App operations from the CLI or HTTP API.
          </li>
        </ul>
        <p>
          If tools do not appear after setup, see{" "}
          <DocsLink to={DOCS_TROUBLESHOOTING_PATH}>Troubleshooting</DocsLink>.
        </p>
      </DocsPageBody>
    </>
  );
}

export function CliDocsPage() {
  const origin = useDeploymentOrigin();

  return (
    <>
      <DocsPageHeader title="Gestalt CLI" />
      <DocsPageBody>
        <p>
          The Gestalt CLI gives you the same capabilities as the browser, from
          the terminal. Use it for scripting and automation, or when you need
          something the UI does not expose, such as choosing among multiple
          connections or calling operations with JSON payloads.
        </p>
        <p>
          Install the CLI, point it at this workspace, then authenticate. This
          page covers setup only. For adding app accounts, see{" "}
          <DocsLink to={DOCS_CONNECT_PATH}>Connect apps</DocsLink>. For admin
          grants, see{" "}
          <DocsLink to={DOCS_AUTHORIZATION_PATH}>Grant App Access</DocsLink>.
          For token lifecycle commands, see{" "}
          <DocsLink to={DOCS_TOKENS_PATH}>API Tokens</DocsLink>. For
          workflow inspection, see{" "}
          <DocsLink to={DOCS_WORKFLOWS_PATH}>Inspect Workflows</DocsLink>.
        </p>
        <WorkspaceUrlBlock origin={origin} />

        <Subheading id="cli-install" />
        <p>
          Install the <Code>gestalt</Code> CLI using one of the methods below.
        </p>
        <MethodCodeSwitcher
          label="Install methods"
          items={[...GESTALT_INSTALL_METHODS]}
        />
        <p>
          Confirm the CLI is on your <Code>PATH</Code> by running:
        </p>
        <CodeBlock chrome="inset" language="cli" code="gestalt --version" />
        <p>
          You should see a version number. If you get “command not found,” open
          a new terminal and try again.
        </p>
        <p>
          Prefer a manual download? Get archives on the{" "}
          <DocsLink href="https://github.com/valon-technologies/gestalt/releases">
            GitHub releases page
          </DocsLink>
          .
        </p>

        <Subheading id="cli-point" />
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

        <Subheading id="cli-authenticate" />
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
              code: `export GESTALT_API_KEY=${DOCS_TOKEN_PLACEHOLDER}`,
              description:
                "Uses an API token directly for scripts, MCP clients, and other non-interactive flows.",
            },
          ]}
        />
        <p>
          Confirm authentication by listing Apps you can reach:
        </p>
        <CodeBlock chrome="inset" language="cli" code="gestalt apps list" />
      </DocsPageBody>
    </>
  );
}

export function ConnectDocsPage() {
  return (
    <>
      <DocsPageHeader title="Connect apps" />
      <DocsPageBody>
        <Subheading id="connect-browser" />
        <p>
          Open{" "}
          <DocsLink to="/apps">Apps</DocsLink>, choose an App, and complete its
          OAuth or manual credential flow. Confirm the App shows as connected
          for your account.{" "}
          <DocsLink to={DOCS_GETTING_STARTED_PATH}>Getting Started</DocsLink>{" "}
          already covers this for first-time setup.
        </p>

        <Subheading id="connect-cli" />
        <p>
          Connecting from the terminal needs the Gestalt CLI. See{" "}
          <DocsLink to={DOCS_CLI_PATH}>Gestalt CLI</DocsLink> to install it,
          then list Apps and start the same OAuth or manual flow from the
          shell.
        </p>
        <CodeBlock chrome="inset"
          language="cli"
          code={`gestalt apps list
gestalt apps connect <app>
gestalt apps connect <app> --connection <name> --instance <instance>`}
        />
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
      <DocsPageHeader title="API Tokens" />
      <DocsPageBody>
        <p>
          An API token is a secret password. Claude Code, ChatGPT, Codex, Cursor,
          and the Gestalt CLI use it to act as you with the Apps you have
          connected.
        </p>
        <p>
          Create one in the browser: open{" "}
          <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>
            Settings → API tokens
          </DocsLink>
          , click Create token, give it a descriptive name, and copy the secret
          immediately. The value is shown only once.
        </p>

        <Subheading id="tokens-use" />
        <p>
          After you copy the secret, open{" "}
          <DocsLink to={DOCS_MCP_PATH}>MCP Clients</DocsLink> and pick your
          assistant. That page has the Claude Code, ChatGPT, Codex, Cursor, and
          Cursor Agent walkthroughs.
        </p>
        <p>
          Do not share the token. Anyone who has it can act as you.
        </p>

        <Subheading id="tokens-cli" />
        <p>
          Creating tokens from the terminal needs the Gestalt CLI. See{" "}
          <DocsLink to={DOCS_CLI_PATH}>Gestalt CLI</DocsLink> to install it,
          then create, list, or revoke tokens from the shell.
        </p>
        <CodeBlock chrome="inset"
          language="cli"
          code={`gestalt tokens create --name automation
gestalt tokens list
gestalt tokens revoke <token-id>`}
        />
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
          workspace-level access to app operations from the Gestalt CLI. Once
          connected, end users can choose the operations they want enabled from
          the app's Connection page. Ask your workspace admin if an operation
          is unavailable.
        </p>
        <p>
          Most teams grant access at the app level. App admins can manage
          members for apps they administer. Built-in Gestalt admins can
          manage every app and the global admin set. If your deployment
          splits public and management listeners, pass{" "}
          <Code>--url &lt;management-url&gt;</Code>{" "}
          to admin authorization commands.
        </p>
        <DocsAudienceCallout />

        <Subheading id="authz-plugin-access" />
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

        <Subheading id="authz-service-accounts" />
        <p>
          Service accounts are managed identities. Create the account, grant it
          an app role, connect the app credentials it needs (same credential
          flow as{" "}
          <Code>gestalt apps connect</Code>, scoped to that
          account), then mint a scoped token for automation.
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

        <Subheading id="authz-admins" />
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

        <Subheading id="authz-inspect" />
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

        <Subheading id="wf-help" />
        <CodeBlock chrome="inset" language="cli" code="gestalt workflows --help" />
        <p>
          In this workspace, the default browser UI focuses on recent workflow
          execution history and durable per-step state.
        </p>

        <Subheading id="wf-runs" />
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
      <DocsPageHeader title="MCP Clients" />
      <DocsPageBody>
        <p>
          Add this Gestalt workspace to Claude Code, ChatGPT, Codex, Cursor,
          Cursor Agent, or another assistant so it can use your linked Apps
          as tools.
        </p>
        <p>
          Pick your assistant below. Each option is the full path: create a
          token, open the app, and place the URL and token where that product
          expects them. You can also create tokens from the terminal (see{" "}
          <DocsLink to={DOCS_TOKENS_PATH}>API Tokens</DocsLink>
          ).
        </p>
        <p>
          Complete{" "}
          <DocsLink to={DOCS_GETTING_STARTED_PATH}>Getting Started</DocsLink>{" "}
          first if you have not connected Apps yet.
        </p>

        <Subheading id="mcp-connect" />
        <AssistantDestinationSwitcher origin={origin} />

        <Subheading id="mcp-overlap" />
        <p>
          Gestalt MCP is the default for company workspace apps. One MCP
          endpoint, your workspace token, and the accounts and grants you set
          up in this UI.
        </p>
        <p>
          Assistant skills are markdown playbooks (runbooks, SQL patterns, team
          conventions). They do not replace Gestalt app accounts. Enable a
          skill when you want procedural guidance, not when you need live API
          access.
        </p>
        <p>
          Codex and other assistants may offer native plugins for the same
          vendor (for example Notion). Those use a separate sign-in and a
          separate tool list. If Notion is connected in Apps here, use Gestalt
          for Notion and leave the Codex Notion plugin off to avoid duplicate
          tools and conflicting auth.
        </p>

        <Subheading id="mcp-env" />
        <p>
          Some assistants read the token from a name on your computer,{" "}
          <Code>GESTALT_API_KEY</Code>, instead of a form field. Store your
          token there. Replace the placeholder with the secret you copied from
          Settings.
        </p>
        <CodeBlock
          chrome="inset"
          language="shellscript"
          code={`export GESTALT_API_KEY=${DOCS_TOKEN_PLACEHOLDER}`}
        />
        <p>
          If you add it to <Code>~/.zshrc</Code>, run{" "}
          <Code>source ~/.zshrc</Code> afterward.
        </p>
        <p>
          If a recipe on this page asks for a host named{" "}
          <Code>GESTALT_URL</Code>, also set:
        </p>
        <CodeBlock
          chrome="inset"
          language="shellscript"
          code={`export GESTALT_URL=${origin}`}
        />
        <p>
          On workspaces with authentication disabled, omit the token from the
          recipes on this page.
        </p>
        <InfoTable
          rows={[
            ["Connect to", `${origin}/mcp`],
            [
              "Sign in with",
              "Your API token (the password for the assistant)",
            ],
            [
              "If no tools appear",
              "Confirm that the App is connected for you on Apps.",
            ],
          ]}
        />
        <Subheading id="mcp-other" />
        <McpOtherClients origin={origin} />

        <Subheading id="mcp-cloud" />
        <p>
          Configure a hosted coding environment when you run agents in the cloud
          rather than on your laptop. Set the workspace URL and API token in
          that environment, then install the CLI in the platform setup or
          startup script.
        </p>
        <AgentEnvironmentTabs origin={origin} />

        <Subheading id="mcp-verify" />
        <p>
          Restart the assistant. Connected Apps should appear as available
          tools. If the list is empty, return to{" "}
          <DocsLink to="/apps">Apps</DocsLink> and confirm the App is connected
          for your account.
        </p>
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
          a missing app account, or a grant that has not taken effect
          yet.
        </p>
        <Subheading id="ts-not-authenticated" />
        <p>
          Run{" "}
          <Code>gestalt auth login</Code>
          , or set{" "}
          <Code>GESTALT_API_KEY</Code>{" "}
          if you are using a token directly.
        </p>

        <Subheading id="ts-multiple-connections" />
        <p>
          Pass{" "}
          <Code>--connection</Code>{" "}
          or{" "}
          <Code>--instance</Code> so
          Gestalt can resolve the correct credentials.
        </p>

        <Subheading id="ts-empty-tools" />
        <p>
          That usually means the app is available in the workspace config
          but has not been linked for your current user yet.
        </p>

        <Subheading id="ts-forbidden" />
        <p>
          Confirm an app admin granted you the expected role (
          <DocsLink to={DOCS_AUTHORIZATION_PATH}>Grant App Access</DocsLink>
          ), that the operation allows that role, and that you are
          authenticated as the same user or service account the grant targets.
          Then retry the invoke or MCP call.
        </p>

        <Subheading id="ts-overlap" />
        <p>
          If the same app appears twice (for example Notion in Gestalt and as a
          Codex plugin), disable the duplicate. Use Gestalt MCP for
          workspace-linked apps. Use native plugins only for gaps Gestalt
          does not cover. See{" "}
          <DocsLink to={DOCS_MCP_PATH} hash="mcp-overlap">
            {ASSISTANT_OVERLAP_TITLE}
          </DocsLink>
          .
        </p>

        <Subheading id="ts-codex-desktop-tools" />
        <p>
          Confirm you ran{" "}
          <Code>codex mcp add</Code> in Terminal on the same Mac as Codex
          Desktop, then restart the app. Check that{" "}
          <Code>GESTALT_API_KEY</Code> is set in the shell where you ran the
          command, or export it in your profile. Full steps:{" "}
          <DocsLink
            to={DOCS_MCP_PATH}
            hash={assistantDocsLandingHash(assistantHostById("codex"))}
          >
            Codex Desktop MCP setup
          </DocsLink>
          .
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
  // Reading contract: prose uses typeset-reading (lists, markers, code).
  // Chrome islands use `.not-typeset` or `[data-typeset-chrome]` (flow gap only).
  // Set h2 start on this node — `.typeset` owns the token and would ignore an
  // inherited value from PageLayout.
  // Title lives outside this node. A leading paragraph uses flow gap; a
  // leading h2 uses the same section gap as later h2s (first-child h2 margin
  // is otherwise zeroed).
  return (
    <div
      className="typeset typeset-docs mt-[length:var(--typeset-flow,1.5em)] has-[>h2:first-child]:mt-[length:var(--typeset-h2-margin-start)]"
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

function MethodCodeSwitcher({
  label,
  items,
}: {
  label: string;
  items: { id: string; label: string; code: string; description: string }[];
}) {
  const ids = items.map((item) => item.id);
  const [activeId, setActiveId] = useHashTab(ids, items[0]?.id ?? "");
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
  const invokeTabIds = ["invoke-cli", "invoke-http"] as const;
  const [activeId, setActiveId] = useHashTab(invokeTabIds, "invoke-cli");

  return (
    <DocsOptionSwitcher
      label="Invocation methods"
      options={[
        { value: "invoke-cli", label: "CLI" },
        { value: "invoke-http", label: "HTTP" },
      ]}
      value={activeId as "invoke-cli" | "invoke-http"}
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
          <CodeBlock chrome="inset" language="cli" code={GESTALT_INSTALL_SCRIPT} />
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
          <CodeBlock chrome="inset" language="cli" code={GESTALT_INSTALL_SCRIPT} />
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

function McpOtherClients({ origin }: { origin: string }) {
  return (
    <>
      <p>
        If the assistant has a connector, tools, or custom MCP screen,
        paste the URL and the token into those fields. If it uses a JSON
        config file, use the example below.
      </p>
      <p>You need:</p>
      <InfoTable
        rows={[
          ["Where to connect", `${origin}/mcp`],
          [
            "How to sign in",
            "Your API token, in the API key or password field",
          ],
          ["If it uses a file", "usually a key named mcpServers"],
        ]}
      />
      <CodeBlock
        chrome="inset"
        language="json"
        code={gestaltMcpClientConfigJson({
          url: `${origin}/mcp`,
          token: DOCS_TOKEN_PLACEHOLDER,
        })}
      />
    </>
  );
}

function useDeploymentOrigin() {
  const [origin, setOrigin] = useState(() => resolveGestaltPublicOrigin());

  useEffect(() => {
    setOrigin(resolveGestaltPublicOrigin());
  }, []);

  return origin;
}

function Subheading({ id }: { id: string }) {
  // Real heading so Registry typeset owns section rhythm (h2 margins /
  // after-heading). `id` and `scroll-mt` must share the same node — hash /
  // scrollIntoView only apply scroll-margin on the matched element.
  // Offset token includes measured sticky chrome (worktree banner + top bar).
  // Title comes from docs-data so TOC labels and headings cannot drift.
  // The `#` is the permalink, not the heading text. Registry Link default
  // (gold ink, draw-underline on hover). `aria-labelledby` keeps the heading
  // name as the title so the `#` stays a separate link.
  const title = docsSubsectionLabel(id);
  const titleId = `${id}-title`;
  return (
    <h2
      id={id}
      className="scroll-mt-[var(--page-layout-anchor-offset)]"
      aria-labelledby={titleId}
    >
      <span id={titleId}>{title}</span>
      <Link
        href={`#${id}`}
        data-heading-permalink
        aria-label={`# Link to ${title}`}
      >
        #
      </Link>
    </h2>
  );
}

function InfoTable({ rows }: { rows: [string, string][] }) {
  return (
    <DescriptionList
      density="condensed"
      termWidth="14rem"
      className="not-typeset mt-[length:var(--typeset-flow,1.5em)]"
      data-testid="docs-info-table"
    >
      {rows.map(([label, value]) => (
        <DescriptionItem key={label}>
          <DescriptionTerm>{label}</DescriptionTerm>
          <DescriptionDetails>{value}</DescriptionDetails>
        </DescriptionItem>
      ))}
    </DescriptionList>
  );
}
