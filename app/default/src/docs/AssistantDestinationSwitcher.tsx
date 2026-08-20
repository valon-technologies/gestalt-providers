import { ASSISTANT_HOST_ICON } from "@/components/assistant-host-icon";
import { CursorIcon } from "@/components/icons";
import { Button } from "@/components/ui/button";
import { CodeBlock } from "@/components/ui/code-block";
import { Code } from "@/components/ui/code";
import type { SegmentedControlOption } from "@/components/ui/segmented-control";
import {
  CHATGPT_INSTALL_DEMO_LABEL,
  CHATGPT_INSTALL_NAME_TYPE,
  CHATGPT_INSTALL_OPEN,
  CHATGPT_INSTALL_PREAMBLE,
  CHATGPT_INSTALL_SAVE,
  CHATGPT_INSTALL_TOKEN,
  CHATGPT_INSTALL_URL,
  CODEX_INSTALL_POSTAMBLE,
  CODEX_INSTALL_PREAMBLE,
  CURSOR_AGENT_INSTALL_PREAMBLE,
} from "@/lib/assistantConnectionCopy";
import { RecipeEmphasis } from "@/lib/recipe-emphasis";
import {
  ASSISTANT_DOCS_LANDING_HASH_ALIASES,
  assistantHostById,
} from "@/lib/assistantHosts";
import { cursorMcpInstallHref } from "@/lib/gestaltMcpClientConfig";
import { appPath } from "@/lib/mount";
import { DocsLink } from "./DocsLink";
import {
  DOCS_MCP_PATH,
  DOCS_SETTINGS_TOKENS_HREF,
} from "./docs-data";
import {
  ASSISTANT_DESTINATION_SWITCHER_LABEL,
  assistantDestinationIds,
  assistantDestinationMedia,
  assistantDestinationTabs,
  defaultAssistantDestinationId,
  DOCS_TOKEN_PLACEHOLDER,
  type AssistantDestinationId,
} from "./assistant-destinations";
import { DocsOptionSwitcher, useHashTab } from "./docs-option-switcher";

export function AssistantDestinationSwitcher({
  origin,
}: {
  origin: string;
}) {
  const mcpUrl = `${origin}/mcp`;
  const [activeTabId, setActiveTabId] = useHashTab(
    assistantDestinationIds,
    defaultAssistantDestinationId,
    ASSISTANT_DOCS_LANDING_HASH_ALIASES,
  );
  const activeId = activeTabId as AssistantDestinationId;

  return (
    <DocsOptionSwitcher
      label={ASSISTANT_DESTINATION_SWITCHER_LABEL}
      options={
        assistantDestinationTabs.map((tab) => {
          const host = assistantHostById(tab.hostId);
          return {
            value: tab.id,
            label: tab.label,
            icon: host ? ASSISTANT_HOST_ICON[host.iconKey] : undefined,
          };
        }) as ReadonlyArray<SegmentedControlOption<AssistantDestinationId>>
      }
      value={activeId}
      onValueChange={setActiveTabId}
      hashAliases={ASSISTANT_DOCS_LANDING_HASH_ALIASES}
    >
      {activeId === "dest-claude-code" ? (
        <ClaudeCodeDestination mcpUrl={mcpUrl} />
      ) : null}
      {activeId === "dest-chatgpt" ? (
        <ChatGptDestination mcpUrl={mcpUrl} />
      ) : null}
      {activeId === "dest-codex" ? (
        <CodexDestination mcpUrl={mcpUrl} />
      ) : null}
      {activeId === "dest-cursor" ? (
        <CursorDestination mcpUrl={mcpUrl} />
      ) : null}
      {activeId === "dest-cursor-agent" ? (
        <CursorAgentDestination origin={origin} />
      ) : null}
      {activeId === "dest-claude" ? (
        <ClaudeDestination mcpUrl={mcpUrl} />
      ) : null}
    </DocsOptionSwitcher>
  );
}

function DestinationTokenStep() {
  return (
    <li>
      Create a token here: open{" "}
      <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>
        Settings → API tokens
      </DocsLink>
      . Click Create token, give it a name, and copy the secret.
    </li>
  );
}

function ClaudeCodeDestination({ mcpUrl }: { mcpUrl: string }) {
  return (
    <>
      <ol>
        <DestinationTokenStep />
        <li>
          Then go here: open a terminal in a project folder where Claude Code
          runs.
        </li>
        <li>
          Place the token here: run the command below, or paste the file. Restart
          Claude Code and confirm Apps appear as tools.
        </li>
      </ol>
      <p>From the terminal:</p>
      <CodeBlock
        chrome="inset"
        language="cli"
        code={`claude mcp add --transport http --scope project \\
  --header "Authorization: Bearer ${DOCS_TOKEN_PLACEHOLDER}" \\
  gestalt "${mcpUrl}"`}
      />
      <p>
        Or put the Gestalt URL and your token in{" "}
        <Code>.mcp.json</Code> (project) or{" "}
        <Code>~/.claude.json</Code> (your user):
      </p>
      <CodeBlock
        language="json"
        filename=".mcp.json"
        code={`{
  "mcpServers": {
    "gestalt": {
      "type": "http",
      "url": "${mcpUrl}",
      "headers": {
        "Authorization": "Bearer ${DOCS_TOKEN_PLACEHOLDER}"
      }
    }
  }
}`}
      />
    </>
  );
}

function ChatGptDestination({ mcpUrl }: { mcpUrl: string }) {
  const demo = assistantHostById("chatgpt")?.installDemo;

  return (
    <>
      <ol>
        <DestinationTokenStep />
      </ol>
      {demo ? (
        <DestinationMedia
          videoSrc={demo.src}
          posterSrc={demo.poster}
          caption={CHATGPT_INSTALL_DEMO_LABEL}
        />
      ) : null}
      <p>{CHATGPT_INSTALL_PREAMBLE}</p>
      <ol>
        <li>
          <RecipeEmphasis text={CHATGPT_INSTALL_OPEN} />
        </li>
        <li>
          <RecipeEmphasis text={CHATGPT_INSTALL_NAME_TYPE} />
        </li>
        <li>
          <span className="block">
            <RecipeEmphasis text={CHATGPT_INSTALL_URL} />
          </span>
          <div className="not-typeset">
            <CodeBlock
              chrome="inset"
              language="plaintext"
              code={mcpUrl}
              copyLabel="Copy MCP URL"
            />
          </div>
        </li>
        <li>
          <span className="block">
            <RecipeEmphasis text={CHATGPT_INSTALL_TOKEN} />
          </span>
          <div className="not-typeset">
            <CodeBlock
              chrome="inset"
              language="plaintext"
              code={DOCS_TOKEN_PLACEHOLDER}
              copyLabel="Copy token placeholder"
            />
          </div>
        </li>
        <li>
          <RecipeEmphasis text={CHATGPT_INSTALL_SAVE} />
        </li>
      </ol>
    </>
  );
}

function CodexDestination({ mcpUrl }: { mcpUrl: string }) {
  return (
    <>
      <ol>
        <DestinationTokenStep />
        <li>
          Then go here: on the machine where Codex Desktop runs, open Terminal
          (not the Codex chat).
        </li>
        <li>
          Place the token here: run the commands below. Restart Codex Desktop
          after adding the server.
        </li>
      </ol>
      <p>{CODEX_INSTALL_PREAMBLE}</p>
      <CodeBlock
        chrome="inset"
        language="cli"
        code={`export GESTALT_API_KEY=${DOCS_TOKEN_PLACEHOLDER}
codex mcp add gestalt --url "${mcpUrl}" --bearer-token-env-var GESTALT_API_KEY`}
      />
      <p>{CODEX_INSTALL_POSTAMBLE}</p>
      <p>
        If authentication is disabled, omit{" "}
        <Code>--bearer-token-env-var GESTALT_API_KEY</Code> from the command.
      </p>
      <p>
        Cloud agents do not use local <Code>codex mcp add</Code>. Configure
        environment variables and the install script in{" "}
        <DocsLink to={DOCS_MCP_PATH} hash="agent-codex">
          Configure Codex Cloud
        </DocsLink>
        .
      </p>
    </>
  );
}

function ClaudeDestination({ mcpUrl }: { mcpUrl: string }) {
  const media = assistantDestinationMedia["dest-claude"];

  return (
    <>
      <ol>
        <DestinationTokenStep />
        <li>
          Then go here: open{" "}
          <DocsLink href="https://claude.ai">claude.ai</DocsLink>
          . Go to Customize, then Connectors. Click Add custom connector. If
          Claude asks for a type, choose Web. On Team or Enterprise, owners add
          the connector under Organization settings, then Connectors.
        </li>
        <li>
          Place the token here: paste the server URL into the server URL field.
          Open Request headers. Header name: <Code>Authorization</Code>. Header
          value: <Code>Bearer</Code>, a space, then your token. Click Add. In a
          chat, click +, then Connectors, and turn Gestalt on.
        </li>
      </ol>
      <DestinationMedia
        videoSrc={media.video}
        posterSrc={media.poster}
        caption={media.caption}
      />
      <p>Server URL:</p>
      <CodeBlock
        chrome="inset"
        language="plaintext"
        code={mcpUrl}
        copyLabel="Copy MCP URL"
      />
      <p>Request headers value (replace the placeholder with your token):</p>
      <CodeBlock
        chrome="inset"
        language="plaintext"
        code={`Bearer ${DOCS_TOKEN_PLACEHOLDER}`}
        copyLabel="Copy Bearer prefix"
      />
    </>
  );
}

function CursorDestination({ mcpUrl }: { mcpUrl: string }) {
  const href = cursorMcpInstallHref(mcpUrl, DOCS_TOKEN_PLACEHOLDER);

  return (
    <>
      <p>
        Create a token in{" "}
        <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>
          Settings → API tokens
        </DocsLink>
        {" "}first. Then click Add in Cursor. If Cursor shows the placeholder,
        paste the secret you copied.
      </p>
      <p className="not-typeset">
        <Button asChild>
          <a href={href} data-testid="docs-add-to-cursor">
            <CursorIcon />
            Add in Cursor
          </a>
        </Button>
      </p>
    </>
  );
}

function CursorAgentDestination({ origin }: { origin: string }) {
  return (
    <>
      <ol>
        <DestinationTokenStep />
        <li>
          Then go here: in your project folder, create or open{" "}
          <Code>.cursor/mcp.json</Code>.
        </li>
        <li>
          Place the token here: paste the config below, then start Agent from
          that folder.
        </li>
      </ol>
      <p>{CURSOR_AGENT_INSTALL_PREAMBLE}</p>
      <CodeBlock
        language="json"
        filename=".cursor/mcp.json"
        code={`{
  "mcpServers": {
    "gestalt": {
      "url": "${origin}/mcp",
      "headers": {
        "Authorization": "Bearer ${DOCS_TOKEN_PLACEHOLDER}"
      }
    }
  }
}`}
      />
    </>
  );
}

function DestinationMedia({
  videoSrc,
  posterSrc,
  caption,
}: {
  videoSrc: string;
  posterSrc: string;
  caption: string;
}) {
  return (
    <figure className="not-typeset">
      <video
        className="w-full rounded-lg border border-border"
        controls
        playsInline
        muted
        loop
        preload="metadata"
        poster={appPath(posterSrc)}
        aria-label={caption}
      >
        <source src={appPath(videoSrc)} type="video/mp4" />
      </video>
      <figcaption className="mt-2 text-center text-sm text-muted-foreground">
        {caption}
      </figcaption>
    </figure>
  );
}
