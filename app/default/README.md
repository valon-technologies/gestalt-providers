# Default app development

The default app keeps its browser API same-origin in every environment. Vite
proxies `/api`, `/theme.css`, and `/theme/*`; the development launcher decides
which backend receives those requests.

## Full-stack workflows

| Mode | Codex skill | Backend | Browser authentication |
| --- | --- | --- | --- |
| Local | `$local-dev` | Per-worktree Gestaltd with an ephemeral local SQLite database | Anonymous loopback session |
| Remote | `$prod-remote` | Deployment declared by a private profile | Authenticated API session; the bearer remains server-side in the supervised loopback proxy |

Both workflows allocate collision-free ports and own only the processes for the
current worktree. Their generated state and logs live under `.local/`.

### Local

The repository-owned contract is
[`../../.engineering-playbook/local-dev.json`](../../.engineering-playbook/local-dev.json).
It starts Gestaltd first with this checkout's default app, then starts Vite with
`GESTALT_API_PROXY_TARGET` bound to that backend. Gestaltd uses released support
providers and synthesizes a fresh SQLite database for the life of the stack;
stopping one worktree does not affect another.

In Codex, invoke the local development skill from anywhere in the worktree:

```text
$local-dev
```

Run `bun install --frozen-lockfile` in this directory once before the first
start. If the workflow is unavailable, the equivalent foreground setup is:

```sh
# Terminal 1, from the repository root
mkdir -p .local
GESTALT_PROVIDERS_DIR= \
  TMPDIR="$PWD/.local" \
  gestaltd serve --port 8080 app/default

# Terminal 2
cd app/default
GESTALT_API_PROXY_TARGET=http://127.0.0.1:8080 \
  VITE_GESTALT_PUBLIC_ORIGIN=http://127.0.0.1:8080 \
  bun run dev
```

`bun run dev` intentionally starts only Vite. Gestaltd owns the provider
runtime when the full stack is running; the frontend package must not leave a
second provider process behind.

### Remote

In Codex, invoke the remote development skill with a deployment-owned or
personal `prod-remote` profile:

```text
$prod-remote
```

The profile is intentionally not included here. Obtain one from the deployment
owner, or use the version 2 template named by the `prod-remote` skill. If the
profile or credential is missing or rejected, rerun the skill and follow its
masked credential-store flow; do not paste a bearer into chat.

The profile owns the remote HTTPS origin, environment classification, Gestaltd
binary, deployment configuration, and credential-store identifiers. Do not
commit those deployment facts or a filled remote profile to this public
repository.

Remote mode is mutation-capable. For production, the profile must explicitly
set `environment` to `production` and `allow_production` to `true`; every start
must use `$prod-remote --confirm-production`. Credentials belong in the native
Gestalt auth store, an explicitly declared OS credential store, or the
workflow's masked standard-input channel—never in `.env.local`, a profile, or a
command argument.

The authenticated remote flow uses an API bearer behind a same-origin loopback
proxy. It validates the app's session endpoint without exposing the bearer to
the browser or Vite client bundle; it does not reproduce the deployment's
interactive OAuth callback on localhost.

## Frontend-only mode

To point Vite at an already-running backend, set its loopback origin explicitly:

```sh
GESTALT_API_PROXY_TARGET=http://127.0.0.1:8080 \
  VITE_GESTALT_PUBLIC_ORIGIN=http://127.0.0.1:8080 \
  bun run dev
```

For tenant theme iteration, keep using `GESTALT_THEME_FILE` as documented in
[`THEMING.md`](THEMING.md). Tenant stylesheets and licensed fonts remain in the
deployment repository.
