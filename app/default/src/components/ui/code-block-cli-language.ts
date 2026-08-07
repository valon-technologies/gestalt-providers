/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 *
 * highlight.js language for CLI *command lines* (docs, install, AI snippets).
 *
 * This is not bash-script highlighting. Plain `gestalt apps list` lines emit no
 * tokens under highlight.js `bash`; this grammar tags the command word, flags,
 * strings, env vars, comments, and line-continuation `\`, using stock `hljs-*`
 * classes so `.typeset-code-hljs` paints them without a second theme.
 *
 * Role → class choices mirror TSX roles in that same theme (not a CLI palette):
 * command → `keyword` (like `import` / `const`), flags → `attr` (like JSX
 * attrs), strings / vars / comments unchanged. Avoid `built_in` for commands —
 * that orange is reserved for builtins (`console`) and made CLI look foreign.
 */

type HljsMode = Record<string, unknown>;

/** @param _hljs highlight.js API (unused — modes are self-contained). */
export default function cli(_hljs: unknown): {
  name: string;
  aliases: string[];
  contains: HljsMode[];
} {
  const VARIABLE: HljsMode = {
    className: "variable",
    variants: [{ begin: /\$\{[\w.:-]+\}/ }, { begin: /\$[\w.:-]+/ }],
  };

  const STRING: HljsMode = {
    className: "string",
    variants: [
      { begin: /'/, end: /'/, illegal: /\n/ },
      { begin: /"/, end: /"/, illegal: /\n/, contains: [VARIABLE] },
    ],
  };

  const FLAG: HljsMode = {
    className: "attr",
    // Require a token boundary so `us-east-1` / `my-app` are not painted as flags.
    begin: /(?<=^|\s)--?[\w-]+/,
  };

  const CONTINUATION: HljsMode = {
    className: "operator",
    begin: /\\$/,
  };

  const TRAILING_COMMENT: HljsMode = {
    className: "comment",
    begin: /\s#[^\n]*/,
  };

  const ARGS: HljsMode[] = [
    CONTINUATION,
    TRAILING_COMMENT,
    STRING,
    VARIABLE,
    FLAG,
  ];

  // Stop before shell list operators so `| jq` / `&& gestalt` can start a new
  // command. Do not treat bare `&` as an operator — that splits `2>&1` and
  // unquoted query strings mid-argument.
  const LIST_OPERATOR = /(?:\|\||&&|[|;])/;
  const LINE_OR_LIST_END = new RegExp(`(?=\\s*${LIST_OPERATOR.source})|$`);

  const COMMAND: HljsMode = {
    className: "keyword",
    begin: /[^\s\\|;]+/,
    starts: {
      end: LINE_OR_LIST_END,
      contains: ARGS,
    },
  };

  return {
    name: "CLI",
    aliases: ["cli", "console-command", "shell-command"],
    contains: [
      {
        className: "comment",
        begin: /^\s*#/,
        end: /$/,
      },
      // Flag-led lines (including unindented continuations like `\n--role`)
      {
        begin: /^\s*(?=--?[\w-])/,
        end: /$/,
        contains: ARGS,
      },
      // Indented continuation lines (positional args, URLs) — not a new command
      {
        begin: /^\s+(?=\S)/,
        end: /$/,
        contains: ARGS,
      },
      {
        begin: new RegExp(`${LIST_OPERATOR.source}\\s*`),
        end: LINE_OR_LIST_END,
        contains: [COMMAND],
      },
      {
        begin: /^(?=\S)/,
        end: LINE_OR_LIST_END,
        contains: [COMMAND],
      },
    ],
  };
}
