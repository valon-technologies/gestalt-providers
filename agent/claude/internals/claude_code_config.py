from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Literal, cast

from claude_agent_sdk.types import SdkPluginConfig

SettingSource = Literal["user", "project", "local"]
SkillDiscovery = Literal["none", "all"]

SUPPORTED_SETTING_SOURCES = frozenset({"user", "project", "local"})
SUPPORTED_SKILL_DISCOVERY = frozenset({"none", "all"})
SUPPORTED_TOOL_BASES = frozenset({"Skill", "Read", "Write", "Bash"})
UNSUPPORTED_LEGACY_FIELDS = frozenset(
    {"claudeCode", "pluginRegistry", "pluginSources", "toolPermissions", "activation"}
)
PASSIVE_PLUGIN_MANIFEST_KEYS = frozenset(
    {"name", "description", "skills", "version", "author", "homepage", "repository", "license"}
)
EXECUTABLE_PLUGIN_MANIFEST_KEYS = frozenset({"mcpServers", "hooks", "commands", "agents", "slashCommands"})
EXECUTABLE_PLUGIN_ROOT_ENTRIES = frozenset(
    {".mcp.json", "settings.json", "settings.local.json", "hooks", "commands", "agents", "bin", ".claude"}
)

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
_SHELL_META_CHARS = re.compile(r"[;&|`$<>\n\r]")
_SKILL_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+(?::[A-Za-z0-9._-]+)?$")


@dataclass(frozen=True, slots=True)
class LocalClaudeCodePlugin:
    manifest_name: str
    path: str


@dataclass(frozen=True, slots=True)
class ToolPermissionSpec:
    raw: str
    base_tool: str
    skill_name: str = ""
    bash_prefix: str = ""

    def allows(self, tool_name: str, arguments: dict[str, Any]) -> bool:
        name = str(tool_name or "").strip()
        if self.base_tool in {"Read", "Write"}:
            return name == self.base_tool
        if self.base_tool == "Skill":
            return self._allows_skill(name, arguments)
        if self.base_tool == "Bash":
            return self._allows_bash(name, arguments)
        return False

    def _allows_skill(self, tool_name: str, arguments: dict[str, Any]) -> bool:
        if tool_name == "Skill":
            requested = _argument_text(arguments, "skill") or _argument_text(arguments, "name")
        elif tool_name.startswith("Skill(") and tool_name.endswith(")"):
            requested = tool_name[len("Skill(") : -1]
        else:
            return False
        if not self.skill_name:
            return True
        return requested == self.skill_name

    def _allows_bash(self, tool_name: str, arguments: dict[str, Any]) -> bool:
        if tool_name != "Bash":
            return False
        if not self.bash_prefix:
            return True
        command = _argument_text(arguments, "command")
        if not command or _SHELL_META_CHARS.search(command):
            return False
        if command == self.bash_prefix:
            return True
        return command.startswith(f"{self.bash_prefix} ")


@dataclass(frozen=True, slots=True)
class ClaudeCodeToolPermissions:
    specs: tuple[ToolPermissionSpec, ...]

    @property
    def configured(self) -> bool:
        return bool(self.specs)

    def allowed_tools(self) -> list[str]:
        return [spec.raw for spec in self.specs]

    def base_tools(self) -> list[str]:
        tools: list[str] = []
        seen: set[str] = set()
        for spec in self.specs:
            if spec.base_tool in seen:
                continue
            seen.add(spec.base_tool)
            tools.append(spec.base_tool)
        return tools

    def allows(self, tool_name: str, arguments: dict[str, Any]) -> bool:
        return any(spec.allows(tool_name, arguments) for spec in self.specs)

    def includes_base_tool(self, tool_name: str) -> bool:
        return any(spec.base_tool == tool_name for spec in self.specs)


@dataclass(frozen=True, slots=True)
class ClaudeCodeTurnOptions:
    plugins: tuple[LocalClaudeCodePlugin, ...]
    setting_sources: tuple[SettingSource, ...]
    disable_auto_memory: bool
    skill_discovery: SkillDiscovery
    skills: tuple[str, ...]
    tool_permissions: ClaudeCodeToolPermissions | None

    @property
    def sdk_skills(self) -> Literal["all"] | list[str]:
        if self.tool_permissions is None or not self.tool_permissions.includes_base_tool("Skill"):
            return []
        if self.skill_discovery == "all":
            return "all"
        return list(self.skills)

    @property
    def sdk_plugins(self) -> list[SdkPluginConfig]:
        return [{"type": "local", "path": plugin.path} for plugin in self.plugins]

    @property
    def plugin_names(self) -> list[str]:
        return [plugin.manifest_name for plugin in self.plugins]

    @property
    def base_tools(self) -> list[str]:
        if self.tool_permissions is None:
            return []
        return self.tool_permissions.base_tools()

    @property
    def allowed_tools(self) -> list[str]:
        if self.tool_permissions is None:
            return []
        return self.tool_permissions.allowed_tools()


@dataclass(frozen=True, slots=True)
class ClaudeCodeConfig:
    setting_sources: tuple[SettingSource, ...] = ()
    disable_auto_memory: bool = True
    skill_discovery: SkillDiscovery = "none"
    skills: tuple[str, ...] = ()
    plugins: tuple[LocalClaudeCodePlugin, ...] = ()
    tool_permissions: ClaudeCodeToolPermissions = ClaudeCodeToolPermissions(specs=())

    @classmethod
    def from_raw(cls, raw_value: Any) -> "ClaudeCodeConfig":
        if raw_value is None:
            return cls()
        if not isinstance(raw_value, dict):
            raise ValueError("Claude provider config must be an object")
        legacy_fields = sorted(set(raw_value) & UNSUPPORTED_LEGACY_FIELDS)
        if legacy_fields:
            raise ValueError(f"unsupported Claude Code config fields: {', '.join(legacy_fields)}")
        if raw_value.get("skills") is not None and raw_value.get("skillDiscovery") is not None:
            raise ValueError("skills and skillDiscovery cannot both be set")

        setting_sources = _parse_setting_sources(raw_value.get("settingSources"))
        disable_auto_memory = _parse_bool(
            raw_value.get("disableAutoMemory"), default=True, field_name="disableAutoMemory"
        )
        skill_discovery = _parse_skill_discovery(raw_value.get("skillDiscovery"))
        skills = _parse_skills(raw_value.get("skills"))
        plugins = _parse_plugins(raw_value.get("plugins"))
        _validate_skill_plugin_prefixes(skills, plugins)
        tool_permissions = _parse_allowed_tools(raw_value.get("allowedTools"))
        return cls(
            setting_sources=setting_sources,
            disable_auto_memory=disable_auto_memory,
            skill_discovery=skill_discovery,
            skills=skills,
            plugins=plugins,
            tool_permissions=tool_permissions,
        )

    @property
    def has_tool_permissions(self) -> bool:
        return self.tool_permissions.configured

    def resolve_turn_options(self, _metadata: dict[str, Any]) -> ClaudeCodeTurnOptions:
        active_permissions = self.tool_permissions if self.tool_permissions.configured else None
        return ClaudeCodeTurnOptions(
            plugins=self.plugins,
            setting_sources=self.setting_sources,
            disable_auto_memory=self.disable_auto_memory,
            skill_discovery=self.skill_discovery,
            skills=self.skills,
            tool_permissions=active_permissions,
        )


def _parse_setting_sources(raw_value: Any) -> tuple[SettingSource, ...]:
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise ValueError("settingSources must be a list")
    values: list[SettingSource] = []
    for raw_item in raw_value:
        item = str(raw_item or "").strip()
        if item not in SUPPORTED_SETTING_SOURCES:
            raise ValueError(f"settingSources entries must be one of {', '.join(sorted(SUPPORTED_SETTING_SOURCES))}")
        values.append(cast(SettingSource, item))
    return tuple(values)


def _parse_skill_discovery(raw_value: Any) -> SkillDiscovery:
    if raw_value is None or str(raw_value).strip() == "":
        return "none"
    value = str(raw_value).strip()
    if value not in SUPPORTED_SKILL_DISCOVERY:
        raise ValueError("skillDiscovery must be one of all, none")
    return cast(SkillDiscovery, value)


def _parse_skills(raw_value: Any) -> tuple[str, ...]:
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise ValueError("skills must be a list of skill names")
    skills: list[str] = []
    for index, raw_item in enumerate(raw_value):
        if not isinstance(raw_item, str) or not raw_item.strip():
            raise ValueError(f"skills[{index}] must be a non-empty string")
        item = raw_item.strip()
        if not _SKILL_NAME_RE.match(item):
            raise ValueError(f"skills[{index}] must be a skill name or plugin:skill qualified name")
        if item in skills:
            raise ValueError(f"skills[{index}] duplicates skills[{skills.index(item)}]")
        skills.append(item)
    return tuple(skills)


def _validate_skill_plugin_prefixes(skills: tuple[str, ...], plugins: tuple[LocalClaudeCodePlugin, ...]) -> None:
    plugin_names = {plugin.manifest_name for plugin in plugins}
    for index, skill in enumerate(skills):
        plugin_name, separator, _ = skill.partition(":")
        if separator and plugin_name not in plugin_names:
            known = ", ".join(sorted(plugin_names)) or "none"
            raise ValueError(f"skills[{index}] references unknown plugin {plugin_name!r}; configured plugins: {known}")


def _parse_plugins(raw_value: Any) -> tuple[LocalClaudeCodePlugin, ...]:
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise ValueError("plugins must be a list of local plugin paths")
    plugins: list[LocalClaudeCodePlugin] = []
    seen_paths: dict[str, int] = {}
    seen_manifest_names: dict[str, int] = {}
    for index, raw_path in enumerate(raw_value, start=1):
        if not isinstance(raw_path, str):
            raise ValueError(f"plugins[{index}] must be a local plugin path")
        plugin = _parse_local_plugin_path(index, raw_path)
        existing_path_index = seen_paths.get(plugin.path)
        if existing_path_index is not None:
            raise ValueError(f"plugins[{index}] and plugins[{existing_path_index}] resolve to the same path")
        existing_name_index = seen_manifest_names.get(plugin.manifest_name)
        if existing_name_index is not None:
            raise ValueError(
                f"plugins[{index}] and plugins[{existing_name_index}] use the same manifest name "
                f"{plugin.manifest_name!r}"
            )
        seen_paths[plugin.path] = index
        seen_manifest_names[plugin.manifest_name] = index
        plugins.append(plugin)
    return tuple(plugins)


def _parse_local_plugin_path(index: int, raw_path: str) -> LocalClaudeCodePlugin:
    path = raw_path.strip()
    if not path:
        raise ValueError(f"plugins[{index}] path is required")
    if not os.path.isabs(path):
        raise ValueError(f"plugins[{index}] path must be absolute")
    canonical_path = os.path.realpath(path)
    if not os.path.isdir(canonical_path):
        raise ValueError(f"plugins[{index}] path must be an existing directory")
    manifest_path = os.path.join(canonical_path, ".claude-plugin", "plugin.json")
    manifest = _read_manifest(index, manifest_path)
    manifest_name = str(manifest.get("name") or "").strip()
    if not manifest_name:
        raise ValueError(f"plugins[{index}] manifest name is required")
    _validate_manifest_components(
        index=index, manifest_name=manifest_name, plugin_path=canonical_path, manifest=manifest
    )
    return LocalClaudeCodePlugin(manifest_name=manifest_name, path=canonical_path)


def _read_manifest(index: int, manifest_path: str) -> dict[str, Any]:
    if not os.path.isfile(manifest_path):
        raise ValueError(f"plugins[{index}] must include .claude-plugin/plugin.json")
    try:
        with open(manifest_path, encoding="utf-8") as handle:
            value = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"plugins[{index}] manifest must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"plugins[{index}] manifest must be a JSON object")
    return value


def _validate_manifest_components(
    *, index: int, manifest_name: str, plugin_path: str, manifest: dict[str, Any]
) -> None:
    executable_manifest_keys = sorted(set(manifest) & EXECUTABLE_PLUGIN_MANIFEST_KEYS)
    if executable_manifest_keys:
        raise ValueError(
            f"plugins[{index}] manifest {manifest_name!r} includes unsupported components: "
            f"{', '.join(executable_manifest_keys)}"
        )
    extra_keys = sorted(set(manifest) - PASSIVE_PLUGIN_MANIFEST_KEYS)
    if extra_keys:
        raise ValueError(
            f"plugins[{index}] manifest {manifest_name!r} includes unsupported components: {', '.join(extra_keys)}"
        )
    unsupported_root_entries = sorted(
        entry for entry in EXECUTABLE_PLUGIN_ROOT_ENTRIES if os.path.exists(os.path.join(plugin_path, entry))
    )
    if unsupported_root_entries:
        raise ValueError(
            f"plugins[{index}] manifest {manifest_name!r} includes unsupported root components: "
            f"{', '.join(unsupported_root_entries)}"
        )
    if "skills" not in manifest and not os.path.isdir(os.path.join(plugin_path, "skills")):
        raise ValueError(f"plugins[{index}] manifest {manifest_name!r} must include skills or a skills directory")


def _parse_allowed_tools(raw_value: Any) -> ClaudeCodeToolPermissions:
    if raw_value is None:
        return ClaudeCodeToolPermissions(specs=())
    if not isinstance(raw_value, list):
        raise ValueError("allowedTools must be a list")
    specs = tuple(_parse_tool_specifier(raw_tool) for raw_tool in raw_value)
    return ClaudeCodeToolPermissions(specs=specs)


def _parse_tool_specifier(raw_value: Any) -> ToolPermissionSpec:
    raw = str(raw_value or "").strip()
    if not raw:
        raise ValueError("allowedTools entries must be non-empty")
    if raw in SUPPORTED_TOOL_BASES:
        return ToolPermissionSpec(raw=raw, base_tool=raw)
    if raw.startswith("Skill(") and raw.endswith(")"):
        skill_name = raw[len("Skill(") : -1].strip()
        if not skill_name or _CONTROL_CHARS.search(skill_name):
            raise ValueError(f"unsupported Claude Code tool specifier {raw!r}")
        return ToolPermissionSpec(raw=raw, base_tool="Skill", skill_name=skill_name)
    if raw.startswith("Bash(") and raw.endswith(")"):
        prefix = _bash_prefix(raw[len("Bash(") : -1])
        return ToolPermissionSpec(raw=raw, base_tool="Bash", bash_prefix=prefix)
    raise ValueError(f"unsupported Claude Code tool specifier {raw!r}")


def _bash_prefix(value: str) -> str:
    if value.endswith(":*"):
        prefix = value[:-2]
    elif value.endswith("*"):
        prefix = value[:-1]
    else:
        raise ValueError("Bash tool specifiers must end with *")
    prefix = prefix.strip()
    if not prefix or _CONTROL_CHARS.search(prefix) or _SHELL_META_CHARS.search(prefix):
        raise ValueError("Bash tool specifier prefixes must be non-empty literal commands")
    return prefix


def _parse_bool(raw_value: Any, *, default: bool, field_name: str) -> bool:
    if raw_value is None:
        return default
    if isinstance(raw_value, bool):
        return raw_value
    raise ValueError(f"{field_name} must be a boolean")


def _argument_text(arguments: dict[str, Any], key: str) -> str:
    value = arguments.get(key)
    return str(value or "").strip() if value is not None else ""
