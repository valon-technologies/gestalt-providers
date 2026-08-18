import { useState, type ComponentType, type ReactNode } from "react";
import { Info, Lightbulb } from "lucide-react";
import { Link } from "@tanstack/react-router";
import {
  AlertActions,
  AlertDescription,
  AlertTitle,
  Callout,
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
  ClaudeCodeIcon,
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
  CURSOR_AGENT_INSTALL_PREAMBLE,
  MCP_SETUP_DOCS_LINK_LABEL,
} from "@/lib/assistantConnectionCopy";
import {
  ASSISTANT_HOST_PICKER_GRID_CLASS,
  ASSISTANT_HOSTS_IN_PICKER,
  assistantHostById,
  type AssistantHost,
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
import {
  gestaltMcpBearerValue,
  gestaltMcpClientConfigJson,
} from "@/lib/gestaltMcpClientConfig";
import { resolveGestaltPublicOrigin } from "@/lib/gestaltPublicOrigin";

function cursorMcpInstallHref(mcpUrl: string, apiToken: string): string {
  const config = {
    url: mcpUrl,
    headers: {
      Authorization: gestaltMcpBearerValue(apiToken),
    },
  };
  const json = JSON.stringify(config);
  const base64 = btoa(json);
  return `cursor://anysphere.cursor-deeplink/mcp/install?name=${encodeURIComponent("gestalt")}&config=${encodeURIComponent(base64)}`;
}

function hostIcon(host: AssistantHost): ReactNode {
  const iconClass = "size-12 shrink-0 text-foreground";
  switch (host.iconKey) {
    case "claude":
      return <ClaudeIcon className="size-12 shrink-0" />;
    case "claude-code":
      return <ClaudeCodeIcon className="size-12 shrink-0" />;
    case "chatgpt":
      return <ChatGptIcon className="size-12 shrink-0" />;
    case "cursor":
      return <CursorIcon className={iconClass} />;
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
      className={ASSISTANT_HOST_PICKER_GRID_CLASS}
      data-testid="build-install-radio"
      aria-label="Choose your assistant"
    >
      {ASSISTANT_HOSTS_IN_PICKER.map((host) => {
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
              {hostIcon(host)}
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
    </RadioGroup>
  );
}

function CreateTokenFirstCallout() {
  return (
    <Callout variant="info" data-testid="build-install-token-needed">
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
    </Callout>
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
}: {
  mcpUrl: string;
  bearerValue: string;
}) {
  return (
    <div className="space-y-4">
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
}: {
  mcpUrl: string;
  tokenValue: string;
}) {
  return (
    <div className="space-y-4">
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

type HostInstallRecipeProps = {
  mcpUrl: string;
  apiToken: string;
  onMarkMcpInstalled: () => void;
};

function CursorMcpConfigBlock({
  mcpUrl,
  apiToken,
}: {
  mcpUrl: string;
  apiToken: string;
}) {
  return (
    <CodeBlock
      variant="outline"
      code={gestaltMcpClientConfigJson({
        url: mcpUrl,
        token: apiToken,
      })}
      language="json"
      filename=".cursor/mcp.json"
    />
  );
}

function CursorInstallRecipe({
  mcpUrl,
  apiToken,
  onMarkMcpInstalled,
}: HostInstallRecipeProps) {
  const [cursorMethod, setCursorMethod] = useState<"open" | "paste">("open");
  const cursorInstallHref = cursorMcpInstallHref(mcpUrl, apiToken);

  return (
    <div className="space-y-4">
      <RadioGroup
        value={cursorMethod}
        onValueChange={(value) => setCursorMethod(value as "open" | "paste")}
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
              <Button asChild>
                <a
                  href={cursorInstallHref}
                  data-testid="build-add-to-cursor"
                  onClick={() => onMarkMcpInstalled()}
                >
                  Add in Cursor
                </a>
              </Button>
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
              <CursorMcpConfigBlock mcpUrl={mcpUrl} apiToken={apiToken} />
            </CollapsibleContent>
          </Collapsible>
        </div>
      </RadioGroup>
    </div>
  );
}

function ClaudeInstallRecipe({ mcpUrl, apiToken }: HostInstallRecipeProps) {
  return (
    <ClaudeConnectorRecipe
      mcpUrl={mcpUrl}
      bearerValue={gestaltMcpBearerValue(apiToken)}
    />
  );
}

function ChatGptInstallRecipe({ mcpUrl, apiToken }: HostInstallRecipeProps) {
  return <ChatGptConnectorRecipe mcpUrl={mcpUrl} tokenValue={apiToken} />;
}

function ClaudeCodeInstallRecipe({
  mcpUrl,
  apiToken,
}: HostInstallRecipeProps) {
  const claudeCodeCommand = `claude mcp add --transport http --scope project \\
  --header "Authorization: Bearer ${apiToken}" \\
  gestalt "${mcpUrl}"`;
  return (
    <div data-testid="build-install-claude-code-snippet">
      <CodeBlock
        variant="outline"
        chrome="inset"
        code={claudeCodeCommand}
        language="bash"
      />
    </div>
  );
}

function CodexInstallRecipe({ mcpUrl, apiToken }: HostInstallRecipeProps) {
  const codexCommand = `export GESTALT_API_KEY=${apiToken}
codex mcp add gestalt --url "${mcpUrl}" --bearer-token-env-var GESTALT_API_KEY`;
  return (
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
  );
}

function CursorAgentInstallRecipe({
  mcpUrl,
  apiToken,
}: HostInstallRecipeProps) {
  return (
    <div className="space-y-4" data-testid="build-install-cursor-agent-recipe">
      <p className="text-sm text-muted-foreground text-pretty">
        {CURSOR_AGENT_INSTALL_PREAMBLE}
      </p>
      <CursorMcpConfigBlock mcpUrl={mcpUrl} apiToken={apiToken} />
    </div>
  );
}

function OtherInstallRecipe({ mcpUrl, apiToken }: HostInstallRecipeProps) {
  const bearerValue = gestaltMcpBearerValue(apiToken);
  const clientConfig = gestaltMcpClientConfigJson({
    url: mcpUrl,
    token: apiToken,
  });
  return (
    <div className="space-y-4" data-testid="build-install-other-recipe">
      <div className="space-y-2">
        <span className="block text-sm font-medium text-foreground">URL</span>
        <div>
          <CopyableCode value={mcpUrl} tooltip="Copy connection URL" />
        </div>
      </div>
      <div className="space-y-2">
        <span className="block text-sm font-medium text-foreground">
          Authorization
        </span>
        <div>
          <CopyableCode
            value={bearerValue}
            tooltip="Copy Authorization value"
          />
        </div>
      </div>
      <CodeBlock
        variant="outline"
        chrome="inset"
        code={clientConfig}
        language="json"
      />
    </div>
  );
}

export const HOST_INSTALL_RECIPES: Record<
  BuildInstallAgentId,
  ComponentType<HostInstallRecipeProps>
> = {
  cursor: CursorInstallRecipe,
  "cursor-agent": CursorAgentInstallRecipe,
  claude: ClaudeInstallRecipe,
  chatgpt: ChatGptInstallRecipe,
  "claude-code": ClaudeCodeInstallRecipe,
  codex: CodexInstallRecipe,
  other: OtherInstallRecipe,
};

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
  const mcpUrl = `${resolveGestaltPublicOrigin()}/mcp`;
  const Recipe = HOST_INSTALL_RECIPES[agent];
  const mcpDocsHash = assistantHostById(agent)?.docsHash ?? "mcp-other";

  return (
    <div className="w-full space-y-4" data-testid="build-mcp-install-single">
      {!hasMcpCredential ? (
        <CreateTokenFirstCallout />
      ) : (
        <Recipe
          mcpUrl={mcpUrl}
          apiToken={apiToken}
          onMarkMcpInstalled={onMarkMcpInstalled}
        />
      )}

      <Callout variant="info" data-testid="setup-overlap-callout">
        <Info aria-hidden="true" />
        <AlertTitle>{ASSISTANT_OVERLAP_TITLE}</AlertTitle>
        <AlertDescription>{ASSISTANT_OVERLAP_SHORT}</AlertDescription>
      </Callout>

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
