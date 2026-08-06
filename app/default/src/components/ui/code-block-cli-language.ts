/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 *
 * highlight.js language for CLI *command lines* (docs, install, AI snippets).
 *
 * This is not bash-script highlighting. Plain `gestalt apps list` lines emit no
 * tokens under highlight.js `bash`; this grammar tags the command word, flags,
 * strings, env vars, comments, and line-continuation `\`, using stock `hljs-*`
 * classes so `.typeset-code-hljs` paints them without a second theme.
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
    begin: /--?[\w-]+/,
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

  // `$` = end of line in highlight.js lexing; also stop before list operators
  // so a following command (`| jq`, `&& gestalt`) can start.
  const LINE_OR_LIST_END = /(?=\s*(?:\|\||&&|[|;&]))|$/;

  const COMMAND: HljsMode = {
    className: "built_in",
    begin: /[^\s\\|;&]+/,
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
        begin: /(?:\|\||&&|[|;&])\s*/,
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
