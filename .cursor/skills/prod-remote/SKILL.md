---
name: prod-remote
description: >-
  Start gestaltd dev against production (server.remote) with per-worktree port
  registry and hot-reload UI. Uses gestalt auth login instead of session cookies.
  Invoke as /prod-remote. Experimental successor to /prod-dev.
disable-model-invocation: true
---

# prod-remote: gestaltd dev against production (per-worktree)

Runs **whatever app you're in** via native **`gestaltd dev`**: local hot-reload UI,
`server.remote` → production (`https://valon.tools`), auth from **`gestalt auth login`**
(or `GESTALT_API_KEY`). This skill only adds what gestaltd lacks: **per-worktree port
registry**, detached lifecycle, and scoped teardown for multi-worktree concurrency.

Successor to legacy `/prod-dev` (cookie-proxy + manual `session_token`). Use
`/prod-remote` to try the native path; keep `/prod-dev` if you need session-cookie auth
or an older gestaltd without `dev`.

**Same layouts as `/local-dev` and `/prod-dev`:**

| Layout | Detected when | Default app |
|---|---|---|
| `toolshed-app` | under `valon-tools/apps/<app>/` | path segment |
| `providers-console` | gestalt-providers worktree | `default` (`app/default/`) |

Deploy config (unless profile overrides `deploy_config_base` / `deploy_config_local`):

- `~/Work/toolshed/valon-tools/deploy/config.yaml`
- `~/Work/toolshed/valon-tools/deploy/local/config.yaml` (`server.remote: https://valon.tools`)

**Preferred entrypoint:** `wt-prod-remote.py start` — detached gestaltd that survives
agent shell exit. Do **not** background gestaltd from a short-lived agent shell.

## Architecture

```
browser → gestaltd (:backend_port, proxies mount path + HMR websockets)
            → providerdev-spawned Vite (GESTALT_DEV_* + GESTALT_DEV_API_PROXY_TOKEN)
            → local gestaltd /api/*
               → server.remote → https://valon.tools
```

Auth: Bearer token from `gestalt auth login` / `GESTALT_API_KEY`, injected server-side
into the Vite `/api` proxy. **No manual `session_token`.**

Open the **gestaltd landing URL** (`http://127.0.0.1:<backend_port><mount>/`) — HMR
works through the gestaltd dev proxy.

## Isolation invariants (multi-worktree)

| Concern | Guarantee |
|---|---|
| Ports | prod-remote gestaltd backend `8400–8499`. Disjoint from local-dev and legacy prod-dev. |
| Registry | `<git-common-dir>/devstack/prod-remote-registry.json`. Key = absolute worktree root. |
| Kill scope | `release --kill` only touches **this** worktree's gestaltd PID/port. |

Helper: **`~/.claude/skills/prod-remote/bin/wt-prod-remote.py`** (install from this repo's
`.cursor/skills/prod-remote/bin/`). Reuses app contracts from `local-dev`.

## Prerequisites

1. **gestaltd with `dev` subcommand** — `gestaltd dev --help` must work.
2. **Gestalt CLI auth** — `gestalt auth login` or `GESTALT_API_KEY`.
3. **toolshed checkout** — `~/Work/toolshed` or `$TOOLSHED`.

## Install (one-time)

Copy the skill into your personal skills directory so `/prod-remote` resolves:

```bash
cp -R .cursor/skills/prod-remote ~/.claude/skills/
chmod +x ~/.claude/skills/prod-remote/bin/wt-prod-remote.py
```

## Step 0 — Refresh gestaltd (background, non-blocking)

```bash
cd ~/Work/gestalt-fingerprint-fix \
  && git stash \
  && git fetch origin \
  && git rebase origin/main \
  && go build -o ~/Work/toolshed/.gestaltd-bin/gestaltd ./gestaltd/cmd/gestaltd \
  && echo "gestaltd binary rebuilt OK" \
  || echo "gestaltd rebuild failed — check ~/Work/gestalt-fingerprint-fix"
```

## Step 1 — Start

```bash
/usr/bin/python3 ~/.claude/skills/prod-remote/bin/wt-prod-remote.py start
```

On success, JSON includes `landing_url`, `backend_port`. Tell Giovanni: **Open `$LAND`**.

If `"ok": false`:

| error | Action |
|---|---|
| `missing_gestalt_credentials` | Run `gestalt auth login` or set `GESTALT_API_KEY` |
| `gestaltd_dev_unsupported` | Rebuild gestaltd from gestalt-fingerprint-fix |
| `missing_toolshed` | Clone toolshed or set `TOOLSHED` |
| `gestaltd_timeout` | Read `app/<app>/.local/gestaltd-prod-remote.log` |

## Stopping

```bash
/usr/bin/python3 ~/.claude/skills/prod-remote/bin/wt-prod-remote.py release --kill
```

## Diagnostics

```bash
/usr/bin/python3 ~/.claude/skills/prod-remote/bin/wt-prod-remote.py status
/usr/bin/python3 ~/.claude/skills/prod-remote/bin/wt-prod-remote.py prepare
```

## vs legacy `/prod-dev`

| | `/prod-remote` | `/prod-dev` |
|---|---|---|
| Backend | local gestaltd + `server.remote` | direct HTTPS to valon.tools |
| Auth | `gestalt auth login` / API key | manual `session_token` cookie |
| UI | gestaltd-supervised Vite | standalone Vite |
