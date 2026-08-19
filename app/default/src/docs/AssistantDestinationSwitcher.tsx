import { CodeBlock } from "@/components/ui/code-block";
import { Code } from "@/components/ui/code";
import { DocsLink } from "./DocsLink";
import { DOCS_SETTINGS_TOKENS_HREF } from "./docs-data";
import {
  ASSISTANT_DESTINATION_SWITCHER_LABEL,
  assistantDestinationIds,
  assistantDestinationMedia,
  assistantDestinationTabs,
  defaultAssistantDestinationId,
  type AssistantDestinationId,
} from "./assistant-destinations";
import { ASSISTANT_DOCS_LANDING_HASH_ALIASES } from "@/lib/assistantHosts";
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
      options={assistantDestinationTabs.map((tab) => ({
        value: tab.id,
        label: tab.label,
      }))}
      value={activeId}
      onValueChange={setActiveTabId}
      hashAliases={ASSISTANT_DOCS_LANDING_HASH_ALIASES}
    >
      {activeId === "dest-claude" ? (
        <ClaudeDestination mcpUrl={mcpUrl} />
      ) : null}
      {activeId === "dest-chatgpt" ? (
        <ChatGptDestination mcpUrl={mcpUrl} />
      ) : null}
      {activeId === "dest-cursor" ? (
        <CursorDestination mcpUrl={mcpUrl} origin={origin} />
      ) : null}
    </DocsOptionSwitcher>
  );
}

function ClaudeDestination({ mcpUrl }: { mcpUrl: string }) {
  const media = assistantDestinationMedia["dest-claude"];

  return (
    <>
      <ol>
        <li>
          Create a token here: open{" "}
          <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>
            Settings → API tokens
          </DocsLink>
          . Click Create token, give it a name, and copy the secret.
        </li>
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
        code="Bearer gst_api_your_token_here"
        copyLabel="Copy Bearer prefix"
      />
    </>
  );
}

function ChatGptDestination({ mcpUrl }: { mcpUrl: string }) {
  const media = assistantDestinationMedia["dest-chatgpt"];

  return (
    <>
      <ol>
        <li>
          Create a token here: open{" "}
          <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>
            Settings → API tokens
          </DocsLink>
          . Click Create token, give it a name, and copy the secret. ChatGPT
          custom connectors need a paid plan.
        </li>
        <li>
          Then go here: open{" "}
          <DocsLink href="https://chatgpt.com">chatgpt.com</DocsLink>
          . Go to Settings, then Security and login, and turn on Developer mode.
          If you do not see it there, look under Settings, then Apps,
          then Advanced settings. In a chat, click +, then create a custom
          connector. Name it Gestalt.
        </li>
        <li>
          Place the token here: paste the server URL into the server URL field.
          For authentication, choose Token or API key. Paste your token into
          that field. Save, then enable the Gestalt connector in the chat.
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
      <p>Token or API key (replace the placeholder with your token):</p>
      <CodeBlock
        chrome="inset"
        language="plaintext"
        code="gst_api_your_token_here"
        copyLabel="Copy token placeholder"
      />
    </>
  );
}

function CursorDestination({
  mcpUrl,
  origin,
}: {
  mcpUrl: string;
  origin: string;
}) {
  return (
    <>
      <ol>
        <li>
          Create a token here: open{" "}
          <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>
            Settings → API tokens
          </DocsLink>
          . Click Create token, give it a name, and copy the secret.
        </li>
        <li>
          Then go here: open Cursor Settings, then MCP. Add a new server, or
          create <Code>.cursor/mcp.json</Code> in your project.
        </li>
        <li>
          Place the token here: set the server URL to the address below. Set
          Authorization to Bearer plus your token, as in the file.
        </li>
      </ol>
      <p>Server URL:</p>
      <CodeBlock
        chrome="inset"
        language="plaintext"
        code={mcpUrl}
        copyLabel="Copy MCP URL"
      />
      <p>
        Authorization in <Code>.cursor/mcp.json</Code> (replace the
        placeholder with your token):
      </p>
      <CodeBlock
        language="json"
        filename=".cursor/mcp.json"
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
        poster={posterSrc}
        aria-label={caption}
      >
        <source src={videoSrc} type="video/mp4" />
      </video>
      <figcaption className="mt-2 text-center text-sm text-muted-foreground">
        {caption}
      </figcaption>
    </figure>
  );
}
