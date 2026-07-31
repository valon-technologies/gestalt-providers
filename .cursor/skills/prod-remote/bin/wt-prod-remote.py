#!/usr/bin/env python3
"""Per-worktree prod-remote: gestaltd dev → server.remote (production) + hot-reload UI.

Uses native `gestaltd dev` (gestalt auth login / GESTALT_API_KEY for remote token).
Adds per-worktree gestaltd port allocation, detached lifecycle, and scoped kill —
the pieces gestaltd does not provide for multi-worktree concurrency.

Registry: <git-common-dir>/devstack/prod-remote-registry.json
Port range: gestaltd backend 8400–8499 (disjoint from /local-dev and legacy /prod-dev).
"""

import argparse
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

SKILL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_DEV_BIN = os.path.join(os.path.dirname(SKILL_DIR), "local-dev", "bin", "wt-devstack.py")
# When installed under .cursor/skills/, local-dev lives in ~/.claude/skills/
if not os.path.isfile(LOCAL_DEV_BIN):
    LOCAL_DEV_BIN = os.path.expanduser("~/.claude/skills/local-dev/bin/wt-devstack.py")

BACKEND_RANGE = (8400, 8499)
REGISTRY_VERSION = 1
DEFAULT_DEPLOY_REL = ("valon-tools", "deploy")
HEALTH_PATH = "/health"
READY_TIMEOUT_SEC = 300

_spec = importlib.util.spec_from_file_location("wt_devstack", LOCAL_DEV_BIN)
wt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wt)


def die(error, **extra):
    payload = {"ok": False, "error": error}
    payload.update(extra)
    json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")
    sys.exit(1)


def prod_remote_registry_paths(common_dir):
    d = os.path.join(common_dir, "devstack")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "prod-remote-registry.json"), os.path.join(d, "registry.lock")


def load_registry(registry_path):
    if not os.path.isfile(registry_path):
        return {
            "version": REGISTRY_VERSION,
            "ranges": {"backend": list(BACKEND_RANGE)},
            "worktrees": {},
        }
    try:
        with open(registry_path) as f:
            reg = json.load(f)
        reg.setdefault("worktrees", {})
        reg.setdefault("ranges", {"backend": list(BACKEND_RANGE)})
        return reg
    except (json.JSONDecodeError, OSError):
        return {
            "version": REGISTRY_VERSION,
            "ranges": {"backend": list(BACKEND_RANGE)},
            "worktrees": {},
        }


def gc_registry(reg):
    dead = []
    for root, e in reg["worktrees"].items():
        if os.path.isdir(root):
            continue
        if wt.pid_alive(e.get("gestaltd_pid"), "gestaltd"):
            continue
        dead.append(root)
    for root in dead:
        del reg["worktrees"][root]
    return dead


def reserved_backend_ports(reg, exclude_root):
    used = set()
    for root, e in reg["worktrees"].items():
        if root == exclude_root:
            continue
        used.add(e["backend_port"])
    return used


def port_listening(port, host=wt.HOST, timeout=0.12):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_for_health(port, seconds=READY_TIMEOUT_SEC):
    url = f"http://{wt.HOST}:{port}{HEALTH_PATH}"
    deadline = time.time() + seconds
    while time.time() < deadline:
        if health_ok(url):
            return True
        time.sleep(0.5)
    return False


def health_ok(url):
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError, ValueError):
        return False


def kill_port_listeners(port):
    try:
        out = subprocess.run(
            ["lsof", "-nP", f"-ti:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            check=False,
        )
        for line in out.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                os.kill(int(line), 15)
            except OSError:
                pass
    except OSError:
        pass


def listen_pid(port):
    try:
        out = subprocess.run(
            ["lsof", "-nP", f"-ti:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            check=False,
        )
        if out.stdout.strip():
            return int(out.stdout.strip().splitlines()[0])
    except (ValueError, OSError):
        pass
    return None


def spawn_detached(cmd, env, log_path, cwd=None):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    log = open(log_path, "a", encoding="utf-8")
    return subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
    )


def gestalt_credentials_path():
    xdg = os.environ.get("XDG_CONFIG_HOME", "").strip()
    if xdg:
        return os.path.join(xdg, "gestalt", "credentials.json")
    return os.path.join(os.path.expanduser("~"), ".config", "gestalt", "credentials.json")


def resolve_gestalt_api_token():
    env_key = os.environ.get("GESTALT_API_KEY", "").strip()
    if env_key:
        return env_key, "GESTALT_API_KEY"
    path = gestalt_credentials_path()
    if not os.path.isfile(path):
        return "", ""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        token = (data.get("api_token") or "").strip()
        if token:
            return token, path
    except (json.JSONDecodeError, OSError):
        pass
    return "", path


def gestaltd_supports_dev(gbin):
    try:
        out = subprocess.run(
            [gbin, "dev", "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        return out.returncode == 0 and "gestaltd dev" in (out.stdout + out.stderr)
    except OSError:
        return False


def resolve_deploy_configs(prof):
    explicit_base = prof.get("deploy_config_base")
    explicit_local = prof.get("deploy_config_local")
    if explicit_base and explicit_local:
        base = explicit_base if os.path.isabs(explicit_base) else os.path.abspath(explicit_base)
        local = explicit_local if os.path.isabs(explicit_local) else os.path.abspath(explicit_local)
        if not os.path.isfile(base):
            die("deploy_config_missing", path=base)
        if not os.path.isfile(local):
            die("deploy_config_missing", path=local)
        return base, local

    toolshed = wt.resolve_toolshed_root()
    if not toolshed:
        die(
            "missing_toolshed",
            hint="clone toolshed to ~/Work/toolshed or set TOOLSHED for deploy config overlays",
        )
    deploy_dir = os.path.join(toolshed, *DEFAULT_DEPLOY_REL)
    base = os.path.join(deploy_dir, "config.yaml")
    local = os.path.join(deploy_dir, "local", "config.yaml")
    if not os.path.isfile(base):
        die("deploy_config_missing", path=base)
    if not os.path.isfile(local):
        die("deploy_config_missing", path=local)
    return base, local


def ensure_ui_deps(ui_dir):
    vite_bin = os.path.join(ui_dir, "node_modules", ".bin", "vite")
    if os.path.isfile(vite_bin):
        return
    if shutil.which("bun") and os.path.isfile(os.path.join(ui_dir, "package.json")):
        subprocess.run(["bun", "install"], cwd=ui_dir, check=True)
        return
    die("missing_node_modules", ui_dir=ui_dir, hint="run bun install in the app ui dir")


def prepare_contract(args):
    cwd = args.dir or os.getcwd()
    worktree_root, _ = wt.resolve_active_worktree_root(cwd)
    common_dir = wt.resolve_common_dir(worktree_root)
    app = args.app or wt.detect_app(cwd, worktree_root=worktree_root)
    if not app:
        die("app_not_detected", cwd=cwd, hint=wt.app_not_detected_hint(cwd))

    prof = wt.resolve_app_contract(app, worktree_root)
    if not prof:
        die(
            "unsupported_app",
            app=app,
            hint=wt.unsupported_app_hint(app, worktree_root),
            profiles_dir=wt.PROFILES_DIR,
        )

    app_dir = prof["app_dir"]
    ui_dir = prof["ui_dir"]
    if not os.path.isdir(app_dir):
        die("app_dir_not_found", app=app, expected=app_dir)

    deploy_base, deploy_local = resolve_deploy_configs(prof)
    registry_path, lock_path = prod_remote_registry_paths(common_dir)
    with wt.RegistryLock(lock_path):
        reg = load_registry(registry_path)
        gc_registry(reg)
        entry = reg["worktrees"].get(worktree_root)
        reused = False
        if entry and entry.get("app") == app:
            reused = True
            backend_port = entry["backend_port"]
        else:
            used = reserved_backend_ports(reg, worktree_root)
            backend_port = wt.first_bindable(tuple(reg["ranges"]["backend"]), used)
            if backend_port is None:
                die("port_exhausted", which="backend", range=reg["ranges"]["backend"])
            entry = {
                "app": app,
                "backend_port": backend_port,
                "created_at": wt.now_iso(),
            }
        raw_landing = (prof.get("landing_path") or "/").strip()
        if raw_landing in (".", "./"):
            raw_landing = "/"
        landing_path = wt.normalize_landing_path(raw_landing)
        entry["worktree_root"] = worktree_root
        entry["layout"] = prof.get("layout")
        entry["landing_url"] = f"http://{wt.HOST}:{backend_port}{landing_path}"
        entry["updated_at"] = wt.now_iso()
        reg["worktrees"][worktree_root] = entry
        wt.save_registry(registry_path, reg)

    wt.ensure_runtime_gitignore(worktree_root, prof["app_rel"])
    local_dir = os.path.join(app_dir, ".local")
    lockfile = os.path.join(local_dir, "gestalt.lock.json")
    artifacts_dir = os.path.join(local_dir, ".gestaltd-art")

    return {
        "app": app,
        "layout": prof.get("layout"),
        "worktree_root": worktree_root,
        "app_dir": app_dir,
        "ui_dir": ui_dir,
        "prof": prof,
        "registry": registry_path,
        "reused": reused,
        "backend_port": backend_port,
        "landing_url": entry["landing_url"],
        "landing_path": landing_path,
        "deploy_base": deploy_base,
        "deploy_local": deploy_local,
        "lockfile": lockfile,
        "artifacts_dir": artifacts_dir,
        "verify_api": prof.get("prod_verify_api", "/api/"),
    }


def build_gestaltd_cmd(c, gbin, remote=None, remote_token=None):
    cmd = [
        gbin,
        "dev",
        "--config",
        c["deploy_base"],
        "--config",
        c["deploy_local"],
        "--port",
        str(c["backend_port"]),
        "--lockfile",
        c["lockfile"],
        "--artifacts-dir",
        c["artifacts_dir"],
        c["app_dir"],
    ]
    if remote:
        cmd.extend(["--remote", remote])
    if remote_token:
        cmd.extend(["--remote-token", remote_token])
    return cmd


def cmd_prepare(args):
    c = prepare_contract(args)
    common_dir = wt.resolve_common_dir(c["worktree_root"])
    gbin = wt.resolve_gestaltd(common_dir)
    token, token_src = resolve_gestalt_api_token()
    wt.pin_statusbar_frontend_link(c["landing_url"])
    json.dump(
        {
            "ok": True,
            "skill": "prod-remote",
            "app": c["app"],
            "layout": c.get("layout"),
            "worktree_root": c["worktree_root"],
            "app_dir": c["app_dir"],
            "ui_dir": c["ui_dir"],
            "registry": c["registry"],
            "reused": c["reused"],
            "backend_port": c["backend_port"],
            "landing_url": c["landing_url"],
            "deploy_base": c["deploy_base"],
            "deploy_local": c["deploy_local"],
            "gestaltd_bin": gbin,
            "gestaltd_dev_supported": gestaltd_supports_dev(gbin) if gbin else False,
            "has_remote_token": bool(token),
            "remote_token_source": token_src or None,
            "verify_api": c["verify_api"],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")


def cmd_start(args):
    c = prepare_contract(args)
    common_dir = wt.resolve_common_dir(c["worktree_root"])
    gbin = wt.resolve_gestaltd(common_dir)
    if not gbin:
        die(
            "missing_gestaltd",
            hint="build gestaltd (gestalt-fingerprint-fix or toolshed .gestaltd-bin) and put it on PATH",
        )
    if not gestaltd_supports_dev(gbin):
        die(
            "gestaltd_dev_unsupported",
            gestaltd_bin=gbin,
            hint="rebuild gestaltd from a recent gestalt checkout that includes `gestaltd dev`",
        )

    token, token_src = resolve_gestalt_api_token()
    if not token and not args.remote_token:
        die(
            "missing_gestalt_credentials",
            credentials_path=gestalt_credentials_path(),
            hint="run `gestalt auth login` or set GESTALT_API_KEY before starting prod-remote",
        )

    ensure_ui_deps(c["ui_dir"])

    backend_port = c["backend_port"]
    landing = c["landing_url"]
    health_url = f"http://{wt.HOST}:{backend_port}{HEALTH_PATH}"

    if port_listening(backend_port) and health_ok(health_url):
        gestaltd_pid = listen_pid(backend_port)
        registry_path, lock_path = prod_remote_registry_paths(common_dir)
        with wt.RegistryLock(lock_path):
            reg = load_registry(registry_path)
            entry = reg["worktrees"][c["worktree_root"]]
            entry["gestaltd_pid"] = gestaltd_pid
            entry["updated_at"] = wt.now_iso()
            wt.save_registry(registry_path, reg)
        wt.pin_statusbar_frontend_link(landing)
        json.dump(
            {
                "ok": True,
                "reused": True,
                "already_running": True,
                "skill": "prod-remote",
                "landing_url": landing,
                "app": c["app"],
                "backend_port": backend_port,
                "gestaltd_pid": gestaltd_pid,
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
        return

    kill_port_listeners(backend_port)

    app_dir = c["app_dir"]
    local_dir = os.path.join(app_dir, ".local")
    os.makedirs(local_dir, exist_ok=True)
    os.makedirs(c["artifacts_dir"], exist_ok=True)
    gestaltd_log = os.path.join(local_dir, "gestaltd-prod-remote.log")

    env = os.environ.copy()
    if not env.get("GITHUB_PAT") and shutil.which("gh"):
        try:
            out = subprocess.run(
                ["gh", "auth", "token"],
                capture_output=True,
                text=True,
                check=False,
            )
            if out.returncode == 0 and out.stdout.strip():
                env["GITHUB_PAT"] = out.stdout.strip()
        except OSError:
            pass

    remote_token = (args.remote_token or "").strip() or token
    cmd = build_gestaltd_cmd(
        c,
        gbin,
        remote=(args.remote or "").strip() or None,
        remote_token=remote_token or None,
    )
    proc = spawn_detached(cmd, env, gestaltd_log, cwd=c["worktree_root"])
    time.sleep(0.5)
    if proc.poll() is not None:
        die("gestaltd_failed", log=gestaltd_log, hint=f"see {gestaltd_log}")

    if not wait_for_health(backend_port):
        die(
            "gestaltd_timeout",
            log=gestaltd_log,
            port=backend_port,
            hint=f"gestaltd did not become healthy on :{backend_port}; see {gestaltd_log}",
        )

    gestaltd_pid = listen_pid(backend_port) or proc.pid
    registry_path, lock_path = prod_remote_registry_paths(common_dir)
    with wt.RegistryLock(lock_path):
        reg = load_registry(registry_path)
        entry = reg["worktrees"][c["worktree_root"]]
        entry["gestaltd_pid"] = gestaltd_pid
        entry["updated_at"] = wt.now_iso()
        wt.save_registry(registry_path, reg)

    wt.pin_statusbar_frontend_link(landing)
    json.dump(
        {
            "ok": True,
            "reused": c["reused"],
            "already_running": False,
            "skill": "prod-remote",
            "landing_url": landing,
            "app": c["app"],
            "backend_port": backend_port,
            "gestaltd_pid": gestaltd_pid,
            "gestaltd_log": gestaltd_log,
            "remote_token_source": token_src or ("--remote-token" if args.remote_token else None),
            "verify_api": c["verify_api"],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")


def cmd_status(args):
    common_dir = wt.resolve_common_dir(args.dir or os.getcwd())
    registry_path, lock_path = prod_remote_registry_paths(common_dir)
    with wt.RegistryLock(lock_path):
        reg = load_registry(registry_path)
        gc_registry(reg)
        wt.save_registry(registry_path, reg)
    rows = []
    for root, e in sorted(reg["worktrees"].items()):
        port = e["backend_port"]
        rows.append(
            {
                "worktree": root,
                "app": e.get("app"),
                "backend": port,
                "gestaltd_alive": wt.pid_alive(e.get("gestaltd_pid"), "gestaltd")
                or port_listening(port),
                "landing_url": e.get("landing_url"),
            }
        )
    if args.json:
        json.dump({"ok": True, "registry": registry_path, "entries": rows}, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return
    print(f"prod-remote registry: {registry_path}")
    if not rows:
        print("  (no worktrees registered)")
    for r in rows:
        state = "live" if r["gestaltd_alive"] else "down"
        print(f"  {r['app']:<14} backend :{r['backend']} [{state}]  {r['landing_url']}  {r['worktree']}")


def cmd_release(args):
    cwd = args.dir or os.getcwd()
    common_dir = wt.resolve_common_dir(cwd)
    registry_path, lock_path = prod_remote_registry_paths(common_dir)
    targets = []
    with wt.RegistryLock(lock_path):
        reg = load_registry(registry_path)
        gc_registry(reg)
        if args.all_dead:
            for root, e in list(reg["worktrees"].items()):
                alive = wt.pid_alive(e.get("gestaltd_pid"), "gestaltd") or port_listening(
                    e["backend_port"]
                )
                if not alive:
                    targets.append(root)
        else:
            root = (
                os.path.realpath(args.worktree)
                if args.worktree
                else wt.resolve_active_worktree_root(cwd)[0]
            )
            if root in reg["worktrees"]:
                targets.append(root)
        for root in targets:
            entry = reg["worktrees"][root]
            if args.kill:
                pid = entry.get("gestaltd_pid")
                if wt.pid_alive(pid, "gestaltd"):
                    try:
                        os.kill(pid, 15)
                    except OSError:
                        pass
                kill_port_listeners(entry.get("backend_port"))
            del reg["worktrees"][root]
        wt.save_registry(registry_path, reg)
    json.dump({"ok": True, "released": targets}, sys.stdout, indent=2)
    sys.stdout.write("\n")


def build_parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    prep = sub.add_parser("prepare", help="detect app, reserve gestaltd port, resolve deploy config")
    prep.add_argument("--app")
    prep.add_argument("--dir")
    prep.set_defaults(func=cmd_prepare)

    start = sub.add_parser("start", help="prepare + launch detached gestaltd dev")
    start.add_argument("--app")
    start.add_argument("--dir")
    start.add_argument("--remote", help="override server.remote")
    start.add_argument("--remote-token", help="override remote API token")
    start.set_defaults(func=cmd_start)

    st = sub.add_parser("status", help="list prod-remote stacks")
    st.add_argument("--json", action="store_true")
    st.add_argument("--dir")
    st.set_defaults(func=cmd_status)

    rel = sub.add_parser("release", help="drop reservation; --kill stops gestaltd")
    rel.add_argument("worktree", nargs="?")
    rel.add_argument("--all-dead", action="store_true")
    rel.add_argument("--kill", action="store_true")
    rel.add_argument("--dir")
    rel.set_defaults(func=cmd_release)
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    args.func(args)
