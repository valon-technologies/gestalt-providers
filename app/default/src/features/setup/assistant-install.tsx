import { useState, type ReactNode } from "react";
import { Info, Lightbulb } from "lucide-react";
import { Link } from "@tanstack/react-router";
import {
  Alert,
  AlertActions,
  AlertDescription,
  AlertIcon,
  AlertTitle,
} from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { CodeBlock } from "@/components/ui/code-block";
import { CopyableCode } from "@/components/ui/copyable-code";
import { Label } from "@/components/ui/label";
import { Link as UiLink } from "@/components/ui/link";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
  Collapsible,
  CollapsibleContent,
} from "@/components/ui/collapsible";
import {
  ChatGptIcon,
  ClaudeIcon,
  CodexIcon,
  CursorIcon,
  MoreHorizontalIcon,
} from "@/components/icons";
import {
  ASSISTANT_OVERLAP_SHORT,
  ASSISTANT_OVERLAP_TITLE,
  CHATGPT_INSTALL_AUTH_NOTE,
  CHATGPT_INSTALL_CREATE_APP,
  CHATGPT_INSTALL_DEVELOPER_MODE,
  CHATGPT_INSTALL_ENABLE,
  CHATGPT_INSTALL_TOKEN,
  CHATGPT_INSTALL_URL,
  CHATGPT_PLUGINS_HREF,
  CLAUDE_CONNECTOR_SETTINGS_HREF,
  CLAUDE_INSTALL_ADD_CONNECTOR,
  CLAUDE_INSTALL_ENABLE,
  CLAUDE_INSTALL_HEADERS_NOTE,
  CLAUDE_INSTALL_OPEN,
  CLAUDE_INSTALL_OPEN_CONNECTORS,
  CLAUDE_INSTALL_REQUEST_HEADER,
  CODEX_INSTALL_POSTAMBLE,
  CODEX_INSTALL_PREAMBLE,
  MCP_SETUP_DOCS_LINK_LABEL,
} from "@/lib/assistantConnectionCopy";
import {
  ASSISTANT_HOST_GROUPS,
  assistantHostDocsHash,
  assistantHostsInGroup,
  type BuildInstallAgentId,
} from "@/lib/assistantHosts";
import {
  choiceCardContentNoIndicatorClassName,
  choiceCardFormFieldsClassName,
  choiceCardFormShellClassName,
  choiceCardHoverClassName,
  choiceCardNoIndicatorClassName,
  choiceCardRadioHiddenClassName,
  radioLabelWrappedDisabledClassName,
} from "@/lib/choice-card-chrome";
import { cn } from "@/lib/cn";
import { DOCS_PATH } from "@/lib/constants";
import { SETUP_PRODUCT_NAME } from "@/lib/buildPaths";

function gestaltMcpBaseUrl(): string {
  const configured = import.meta.env.VITE_GESTALT_PUBLIC_ORIGIN?.trim();
  if (configured) return configured.replace(/\/$/, "");
  if (typeof window === "undefined") return "https://your-gestalt-host";
  const { origin, hostname } = window.location;
  if (hostname === "localhost" || hostname === "127.0.0.1") {
    return "https://your-gestalt-host";
  }
  return origin;
}

function cursorMcpInstallHref(mcpUrl: string, apiToken: string): string {
  const config = {
    url: mcpUrl,
    headers: {
      Authorization: `Bearer ${apiToken}`,
    },
  };
  const json = JSON.stringify(config);
  const base64 = btoa(json);
  return `cursor://anysphere.cursor-deeplink/mcp/install?name=${encodeURIComponent("gestalt")}&config=${encodeURIComponent(base64)}`;
}

function hostIcon(id: BuildInstallAgentId): ReactNode {
  switch (id) {
    case "claude":
    case "claude-code":
      return <ClaudeIcon className="size-12 shrink-0 text-[#D97757]" />;
    case "chatgpt":
      return <ChatGptIcon className="size-12 shrink-0 text-foreground" />;
    case "cursor":
      return <CursorIcon className="size-12 shrink-0 text-foreground" />;
    case "codex":
      return <CodexIcon className="size-12 shrink-0" />;
    case "other":
      return (
        <span className="flex size-12 shrink-0 items-center justify-center">
          <MoreHorizontalIcon className="size-6 text-muted-foreground" />
        </span>
      );
  }
}

function hostGridClass(groupId: string): string {
  if (groupId === "coding") {
    return "grid grid-cols-2 gap-3 sm:grid-cols-3";
  }
  return "grid grid-cols-2 gap-3";
}

export function AssistantPickerStepActions({
  selectedAgent,
  onSelectedAgent,
}: {
  selectedAgent: BuildInstallAgentId | "";
  onSelectedAgent: (id: BuildInstallAgentId) => void;
}) {
  return (
    <RadioGroup
      value={selectedAgent || undefined}
      onValueChange={(value) => onSelectedAgent(value as BuildInstallAgentId)}
      className="flex flex-col gap-6"
      data-testid="build-install-radio"
      aria-label="Choose your assistant"
    >
      {ASSISTANT_HOST_GROUPS.map((group) => {
        const headingId = `assistant-group-${group.id}`;
        return (
          <div
            key={group.id}
            role="group"
            aria-labelledby={headingId}
            className="space-y-2"
          >
            <p
              id={headingId}
              className={
                group.id === "other"
                  ? "sr-only"
                  : "text-sm font-medium text-foreground"
              }
            >
              {group.label}
            </p>
            <div className={hostGridClass(group.id)}>
              {assistantHostsInGroup(group.id).map((host) => {
                const inputId = `build-assistant-${host.id}`;
                return (
                  <Label
                    key={host.id}
                    htmlFor={inputId}
                    data-testid={host.testId}
                    className={cn(
                      choiceCardNoIndicatorClassName,
                      choiceCardHoverClassName,
                      "h-full items-center text-center",
                    )}
                  >
                    <RadioGroupItem
                      focusRing="none"
                      value={host.id}
                      id={inputId}
                      className={choiceCardRadioHiddenClassName}
                      aria-label={host.label}
                    />
                    <div
                      className={cn(
                        choiceCardContentNoIndicatorClassName,
                        "items-center gap-2.5",
                      )}
                    >
                      {hostIcon(host.id)}
                      <span
                        data-choice-title
                        className="text-sm font-medium text-foreground"
                      >
                        {host.label}
                      </span>
                    </div>
                  </Label>
                );
              })}
            </div>
          </div>
        );
      })}
    </RadioGroup>
  );
}

function CreateTokenFirstAlert() {
  return (
    <Alert variant="info" data-testid="build-install-token-needed">
      <AlertTitle>Create a token first</AlertTitle>
      <AlertDescription>
        We can only add Gestalt with a token created in this session. Existing
        tokens cannot be shown again.
      </AlertDescription>
      <AlertActions>
        <Button asChild variant="secondary" size="sm">
          <Link to="/setup/$stepId" params={{ stepId: "token" }}>
            Create a token
          </Link>
        </Button>
      </AlertActions>
    </Alert>
  );
}

function RecipeSteps({
  children,
  testId,
}: {
  children: ReactNode;
  testId: string;
}) {
  return (
    <ol
      className="list-decimal space-y-3 pl-5 text-sm text-pretty text-foreground"
      data-testid={testId}
    >
      {children}
    </ol>
  );
}

function ClaudeConnectorRecipe({
  mcpUrl,
  bearerValue,
  hasMcpCredential,
}: {
  mcpUrl: string;
  bearerValue: string;
  hasMcpCredential: boolean;
}) {
  return (
    <div className="space-y-4">
      {!hasMcpCredential ? <CreateTokenFirstAlert /> : null}
      <RecipeSteps testId="build-install-claude-recipe">
        <li>{CLAUDE_INSTALL_OPEN}</li>
        <li>
          {CLAUDE_INSTALL_OPEN_CONNECTORS}{" "}
          <UiLink
            href={CLAUDE_CONNECTOR_SETTINGS_HREF}
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm"
          >
            Open Connectors
          </UiLink>
        </li>
        <li className="space-y-2">
          <span className="block">{CLAUDE_INSTALL_ADD_CONNECTOR}</span>
          <div>
            <CopyableCode value={mcpUrl} tooltip="Copy connection URL" />
          </div>
        </li>
        <li className="space-y-2">
          <span className="block">{CLAUDE_INSTALL_REQUEST_HEADER}</span>
          <div>
            <CopyableCode value={bearerValue} tooltip="Copy Authorization value" />
          </div>
        </li>
        <li>{CLAUDE_INSTALL_ENABLE}</li>
      </RecipeSteps>
      <p className="text-sm text-muted-foreground text-pretty">
        {CLAUDE_INSTALL_HEADERS_NOTE}
      </p>
    </div>
  );
}

function ChatGptConnectorRecipe({
  mcpUrl,
  tokenValue,
  hasMcpCredential,
}: {
  mcpUrl: string;
  tokenValue: string;
  hasMcpCredential: boolean;
}) {
  return (
    <div className="space-y-4">
      {!hasMcpCredential ? <CreateTokenFirstAlert /> : null}
      <RecipeSteps testId="build-install-chatgpt-recipe">
        <li>{CHATGPT_INSTALL_DEVELOPER_MODE}</li>
        <li>
          {CHATGPT_INSTALL_CREATE_APP}{" "}
          <UiLink
            href={CHATGPT_PLUGINS_HREF}
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm"
          >
            Open Plugins
          </UiLink>
        </li>
        <li className="space-y-2">
          <span className="block">{CHATGPT_INSTALL_URL}</span>
          <div>
            <CopyableCode value={mcpUrl} tooltip="Copy connection URL" />
          </div>
        </li>
        <li className="space-y-2">
          <span className="block">{CHATGPT_INSTALL_TOKEN}</span>
          <div>
            <CopyableCode value={tokenValue} tooltip="Copy token" />
          </div>
        </li>
        <li>{CHATGPT_INSTALL_ENABLE}</li>
      </RecipeSteps>
      <p className="text-sm text-muted-foreground text-pretty">
        {CHATGPT_INSTALL_AUTH_NOTE}
      </p>
    </div>
  );
}

export function SingleAgentMcpInstall({
  agent,
  apiToken,
  hasMcpCredential,
  onMarkMcpInstalled,
}: {
  agent: BuildInstallAgentId;
  apiToken: string;
  hasMcpCredential: boolean;
  onMarkMcpInstalled: () => void;
}) {
  const mcpBase = gestaltMcpBaseUrl();
  const mcpUrl = `${mcpBase}/mcp`;
  const tokenForSnippets = hasMcpCredential ? apiToken : "gst_api_YOUR_TOKEN";
  const bearerValue = `Bearer ${tokenForSnippets}`;
  const cursorInstallHref = hasMcpCredential
    ? cursorMcpInstallHref(mcpUrl, apiToken)
    : null;
  const [cursorMethod, setCursorMethod] = useState<"open" | "paste">(
    hasMcpCredential ? "open" : "paste",
  );

  const cursorConfig = `{
  "mcpServers": {
    "gestalt": {
      "url": "${mcpUrl}",
      "headers": {
        "Authorization": "Bearer ${tokenForSnippets}"
      }
    }
  }
}`;

  const claudeCodeCommand = `claude mcp add --transport http --scope project \\
  --header "Authorization: Bearer ${tokenForSnippets}" \\
  gestalt "${mcpUrl}"`;

  const codexCommand = `export GESTALT_API_KEY=${tokenForSnippets}
codex mcp add gestalt --url "${mcpUrl}" --bearer-token-env-var GESTALT_API_KEY`;

  const mcpDocsHash = assistantHostDocsHash(agent);

  return (
    <div className="w-full space-y-4" data-testid="build-mcp-install-single">
      {agent === "cursor" ? (
        <div className="space-y-4">
          {!hasMcpCredential ? <CreateTokenFirstAlert /> : null}

          <RadioGroup
            value={cursorMethod}
            onValueChange={(value) =>
              setCursorMethod(value as "open" | "paste")
            }
            className="flex max-w-xl flex-col gap-2"
            data-testid="build-install-cursor-method"
            aria-label="How to add Gestalt in Cursor"
          >
            <div className={choiceCardFormShellClassName}>
              <Label
                htmlFor="build-install-open-cursor"
                className={cn(
                  "flex cursor-pointer flex-col gap-1 p-4 leading-normal",
                  radioLabelWrappedDisabledClassName,
                )}
              >
                <RadioGroupItem
                  focusRing="none"
                  value="open"
                  id="build-install-open-cursor"
                  disabled={!hasMcpCredential}
                  className={choiceCardRadioHiddenClassName}
                  aria-label="Open Cursor"
                />
                <div className={choiceCardContentNoIndicatorClassName}>
                  <span
                    data-choice-title
                    className="text-sm font-medium text-foreground"
                  >
                    Open Cursor
                  </span>
                  <span
                    data-choice-desc
                    className="text-sm font-normal text-muted-foreground"
                  >
                    Adds Gestalt in one click.
                  </span>
                </div>
              </Label>
              <Collapsible open={cursorMethod === "open"}>
                <CollapsibleContent
                  className={choiceCardFormFieldsClassName}
                  onPointerDown={(event) => event.stopPropagation()}
                >
                  {cursorInstallHref ? (
                    <Button asChild>
                      <a
                        href={cursorInstallHref}
                        data-testid="build-add-to-cursor"
                        onClick={() => onMarkMcpInstalled()}
                      >
                        Add in Cursor
                      </a>
                    </Button>
                  ) : (
                    <Button
                      type="button"
                      disabled
                      data-testid="build-add-to-cursor"
                      title="Create a token first so Cursor can sign in."
                    >
                      Add in Cursor
                    </Button>
                  )}
                </CollapsibleContent>
              </Collapsible>
            </div>

            <div className={choiceCardFormShellClassName}>
              <Label
                htmlFor="build-install-paste-cursor"
                className={cn(
                  "flex cursor-pointer flex-col gap-1 p-4 leading-normal",
                  radioLabelWrappedDisabledClassName,
                )}
              >
                <RadioGroupItem
                  focusRing="none"
                  value="paste"
                  id="build-install-paste-cursor"
                  className={choiceCardRadioHiddenClassName}
                  aria-label="Paste the config yourself"
                />
                <div className={choiceCardContentNoIndicatorClassName}>
                  <span
                    data-choice-title
                    className="text-sm font-medium text-foreground"
                  >
                    Paste the config yourself
                  </span>
                  <span
                    data-choice-desc
                    className="text-sm font-normal text-muted-foreground"
                  >
                    Copy this into Cursor if you cannot use the button.
                  </span>
                </div>
              </Label>
              <Collapsible open={cursorMethod === "paste"}>
                <CollapsibleContent
                  className={choiceCardFormFieldsClassName}
                  onPointerDown={(event) => event.stopPropagation()}
                >
                  <CodeBlock
                    variant="outline"
                    code={cursorConfig}
                    language="json"
                    filename=".cursor/mcp.json"
                  />
                </CollapsibleContent>
              </Collapsible>
            </div>
          </RadioGroup>
        </div>
      ) : null}

      {agent === "claude" ? (
        <ClaudeConnectorRecipe
          mcpUrl={mcpUrl}
          bearerValue={bearerValue}
          hasMcpCredential={hasMcpCredential}
        />
      ) : null}

      {agent === "chatgpt" ? (
        <ChatGptConnectorRecipe
          mcpUrl={mcpUrl}
          tokenValue={tokenForSnippets}
          hasMcpCredential={hasMcpCredential}
        />
      ) : null}

      {agent === "claude-code" ? (
        <div data-testid="build-install-claude-code-snippet">
          <CodeBlock
            variant="outline"
            chrome="inset"
            code={claudeCodeCommand}
            language="bash"
          />
        </div>
      ) : null}

      {agent === "codex" ? (
        <div className="space-y-4" data-testid="build-install-codex-snippet">
          <p className="text-sm text-muted-foreground text-pretty">
            {CODEX_INSTALL_PREAMBLE}
          </p>
          <CodeBlock
            variant="outline"
            chrome="inset"
            code={codexCommand}
            language="bash"
          />
          <p className="text-sm text-muted-foreground text-pretty">
            {CODEX_INSTALL_POSTAMBLE}
          </p>
        </div>
      ) : null}

      {agent === "other" ? (
        <div className="space-y-2">
          <CopyableCode value={mcpUrl} tooltip="Copy connection URL" />
        </div>
      ) : null}

      <Alert variant="info" live={false} data-testid="setup-overlap-callout">
        <AlertIcon>
          <Info aria-hidden="true" />
        </AlertIcon>
        <AlertTitle>{ASSISTANT_OVERLAP_TITLE}</AlertTitle>
        <AlertDescription>{ASSISTANT_OVERLAP_SHORT}</AlertDescription>
      </Alert>

      <p className="flex items-start gap-2 text-sm text-muted-foreground text-pretty">
        <Lightbulb className="mt-0.5 size-4 shrink-0" aria-hidden />
        <span>
          This lets your assistant talk to {SETUP_PRODUCT_NAME} with your
          token. See the{" "}
          <UiLink asChild className="text-sm">
            <Link to={`${DOCS_PATH}/mcp`} hash={mcpDocsHash}>
              {MCP_SETUP_DOCS_LINK_LABEL}
            </Link>
          </UiLink>{" "}
          if you need more detail.
        </span>
      </p>
    </div>
  );
}
