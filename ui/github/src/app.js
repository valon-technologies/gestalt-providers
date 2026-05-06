"use strict";

const PREFERENCE_FIELDS = ["allow_code_review_comments", "self_fix_mode"];
const FIELD_LABELS = {
  allow_code_review_comments: "Inline code review comments",
  self_fix_mode: "Self-fix mode",
};

const state = {
  targets: null,
  selectedRepository: "",
  drafts: new Map(),
  loading: true,
  saving: false,
  error: "",
  saved: false,
};

const app = document.getElementById("app");

function apiPath(path) {
  return path;
}

async function fetchJSON(path, options = {}) {
  const response = await fetch(apiPath(path), {
    ...options,
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });

  if (!response.ok) {
    let message = await response.text();
    try {
      const parsed = JSON.parse(message);
      message = parsed.error || message;
    } catch {
      // Keep the plain response body.
    }
    const error = new Error(message || `Request failed with ${response.status}`);
    error.status = response.status;
    throw error;
  }

  const contentType = response.headers.get("content-type") || "";
  if (!/\bapplication\/([a-z\d.+-]*\+)?json\b/i.test(contentType)) {
    throw new Error(`Expected JSON response, received ${contentType || "unknown content type"}`);
  }

  return response.json();
}

function preferenceValueLabel(value) {
  if (value === true) return "On";
  if (value === false) return "Off";
  if (typeof value === "string") return labelForEnumValue(value);
  return "Default";
}

function labelForEnumValue(value) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function controlKey(repository, control) {
  return `${repository}\u0000${control.policy_id}\u0000${control.field}`;
}

function controlIdentityKind(control) {
  return control.identity_kind === "subject_id" ? "subject_id" : "external_subject_id";
}

function controlValue(repository, control) {
  const key = controlKey(repository, control);
  return state.drafts.has(key)
    ? state.drafts.get(key)
    : normalizePreferenceValue(control, control.stored);
}

function normalizePreferenceValue(control, value) {
  if (control?.type === "enum") {
    return typeof value === "string" && value ? value : null;
  }
  return typeof value === "boolean" ? value : null;
}

function selectedRepository() {
  const repositories = state.targets?.repositories || [];
  return (
    repositories.find((repository) => repository.repository === state.selectedRepository) ||
    repositories[0] ||
    null
  );
}

function isDirty(repository) {
  if (!repository) return false;
  return repository.controls.some((control) => {
    const key = controlKey(repository.repository, control);
    return (
      state.drafts.has(key) &&
      state.drafts.get(key) !== normalizePreferenceValue(control, control.stored)
    );
  });
}

function updateDraft(repository, control, value) {
  const normalized = normalizePreferenceValue(control, value);
  const stored = normalizePreferenceValue(control, control.stored);
  const key = controlKey(repository, control);
  state.saved = false;
  if (normalized === stored) {
    state.drafts.delete(key);
  } else {
    state.drafts.set(key, normalized);
  }
  render();
}

async function loadTargets(options = {}) {
  const preserveSaved = options.preserveSaved === true;
  state.loading = options.showLoading !== false;
  state.error = "";
  if (!preserveSaved) {
    state.saved = false;
  }
  render();

  try {
    const response = await fetchJSON("/api/v1/github/actionPreferences.listTargets");
    const targets = response.data || { repositories: [] };
    state.targets = {
      ...targets,
      repositories: Array.isArray(targets.repositories) ? targets.repositories : [],
    };
    state.drafts.clear();
    if (
      state.selectedRepository &&
      state.targets.repositories.some(
        (repository) => repository.repository === state.selectedRepository,
      )
    ) {
      return;
    }
    state.selectedRepository = state.targets.repositories[0]?.repository || "";
  } catch (error) {
    state.targets = null;
    state.error = preferenceLoadError(error);
  } finally {
    state.loading = false;
    state.saving = false;
    render();
  }
}

function preferenceLoadError(error) {
  const status = Number(error?.status || 0);
  if (status === 412) {
    return error.message || "GitHub action preferences are unavailable.";
  }
  if (status === 404 || status === 405) {
    return "GitHub action preferences are not available in this deployment.";
  }
  if (status === 401) {
    return "Your session expired. Sign in again to manage GitHub settings.";
  }
  return error instanceof Error ? error.message : "Unable to load GitHub settings.";
}

async function savePreferences() {
  const repository = selectedRepository();
  if (!repository || !isDirty(repository)) return;

  state.saving = true;
  state.error = "";
  state.saved = false;
  render();

  try {
    const controlsByPolicy = new Map();
    for (const control of repository.controls) {
      const groupKey = `${control.policy_id}\u0000${controlIdentityKind(control)}`;
      const controls = controlsByPolicy.get(groupKey) || [];
      controls.push(control);
      controlsByPolicy.set(groupKey, controls);
    }

    for (const [groupKey, controls] of controlsByPolicy) {
      const [policyID, identityKind] = groupKey.split("\u0000");
      const values = {};
      let policyDirty = false;
      for (const control of controls) {
        const value = controlValue(repository.repository, control);
        values[control.field] = value;
        policyDirty = policyDirty || value !== normalizePreferenceValue(control, control.stored);
      }
      if (!policyDirty) continue;

      const allDefault = PREFERENCE_FIELDS.every((field) => values[field] == null);
      if (allDefault) {
        await fetchJSON("/api/v1/github/actionPreferences.delete", {
          method: "POST",
          body: JSON.stringify({
            repository: repository.repository,
            policy_id: policyID,
            identity_kind: identityKind,
          }),
        });
      } else {
        await fetchJSON("/api/v1/github/actionPreferences.set", {
          method: "POST",
          body: JSON.stringify({
            repository: repository.repository,
            policy_id: policyID,
            identity_kind: identityKind,
            allow_code_review_comments: values.allow_code_review_comments ?? null,
            self_fix_mode: values.self_fix_mode ?? null,
          }),
        });
      }
    }

    state.saved = true;
    await loadTargets({ preserveSaved: true, showLoading: false });
  } catch (error) {
    state.error = error instanceof Error ? error.message : "Unable to save preferences.";
    state.saving = false;
    render();
  }
}

function render() {
  const repository = selectedRepository();
  app.innerHTML = "";
  app.appendChild(
    element("section", { className: "panel", "aria-live": "polite" }, [
      header(),
      state.loading ? loadingView() : contentView(repository),
    ]),
  );
}

function header() {
  return element("div", { className: "panel-header" }, [
    element("div", {}, [
      element("p", { className: "eyebrow" }, ["GitHub"]),
      element("h1", {}, ["GitHub Bot Actions"]),
    ]),
    element("a", { className: "back-link", href: "/integrations" }, ["Integrations"]),
  ]);
}

function loadingView() {
  return element("p", { className: "muted", style: "margin-top: 22px" }, ["Loading..."]);
}

function contentView(repository) {
  if (state.error && !state.targets) {
    return element("div", { className: "error-panel message-row" }, [
      element("div", {}, [
        element("h2", {}, ["Settings unavailable"]),
        element("p", { className: "muted" }, [state.error]),
      ]),
      retryButton(),
    ]);
  }

  const repositories = state.targets?.repositories || [];
  if (repositories.length === 0) {
    return element("div", { className: "empty message-row" }, [
      element("div", {}, [
        element("h2", {}, ["No configurable repositories"]),
        element("p", { className: "muted" }, [
          "No GitHub repositories currently expose bot action controls for this identity.",
        ]),
      ]),
      retryButton(),
    ]);
  }

  return element("div", {}, [
    toolbar(repositories, repository),
    repository?.html_url
      ? element("p", { className: "repository-meta faint" }, [
          element("a", { className: "back-link", href: repository.html_url, target: "_blank", rel: "noreferrer" }, [
            repository.repository,
          ]),
        ])
      : element("p", { className: "repository-meta faint" }, [repository?.repository || ""]),
    controls(repository),
    element(
      "p",
      {
        className: `status ${state.error ? "error" : state.saved ? "success" : ""}`,
      },
      [state.error || (state.saved ? "Saved" : "")],
    ),
  ]);
}

function toolbar(repositories, repository) {
  const dirty = isDirty(repository);
  return element("div", { className: "toolbar" }, [
    element("label", { className: "select-label" }, [
      "Repository",
      repositorySelect(repositories),
    ]),
    element(
      "button",
      {
        className: "button",
        type: "button",
        disabled: !dirty || state.saving ? "true" : null,
        onclick: savePreferences,
      },
      [state.saving ? "Saving..." : "Save"],
    ),
  ]);
}

function repositorySelect(repositories) {
  const select = element("select", { className: "repository-select" });
  select.value = state.selectedRepository;
  select.addEventListener("change", (event) => {
    state.saved = false;
    state.selectedRepository = event.target.value;
    render();
  });
  for (const repository of repositories) {
    select.appendChild(
      element("option", { value: repository.repository }, [repository.repository]),
    );
  }
  return select;
}

function controls(repository) {
  if (!repository) return element("div");
  return element(
    "div",
    { className: "controls" },
    repository.controls.map((control) => controlRow(repository.repository, control)),
  );
}

function controlRow(repository, control) {
  const value = controlValue(repository, control);
  const label = control.label || FIELD_LABELS[control.field] || control.field;
  return element("section", { className: "control-row" }, [
    element("div", { className: "control-copy" }, [
      element("h3", {}, [label]),
      control.description ? element("p", { className: "muted" }, [control.description]) : null,
      element("p", { className: "faint effective" }, [
        `Effective: ${preferenceValueLabel(normalizePreferenceValue(control, control.effective))}`,
      ]),
    ]),
    preferenceControl(repository, control, value, label),
  ]);
}

function preferenceControl(repository, control, value, label) {
  if (control.type === "enum") {
    return enumControl(repository, control, value, label);
  }
  return segmentedControl(repository, control, value, label);
}

function enumControl(repository, control, value, label) {
  const options = Array.isArray(control.options) ? control.options : [];
  const select = element("select", {
    className: "mode-select",
    "aria-label": `${label} for ${repository}`,
  });
  select.appendChild(element("option", { value: "" }, ["Default"]));
  for (const option of options) {
    if (!option || typeof option.value !== "string") continue;
    select.appendChild(
      element("option", { value: option.value }, [
        typeof option.label === "string" ? option.label : labelForEnumValue(option.value),
      ]),
    );
  }
  select.value = value ?? "";
  select.addEventListener("change", (event) => {
    updateDraft(repository, control, event.target.value || null);
  });
  return select;
}

function segmentedControl(repository, control, value, label) {
  return element(
    "div",
    {
      className: "segmented",
      role: "group",
      "aria-label": `${label} for ${repository}`,
    },
    [null, true, false].map((option) =>
      element(
        "button",
        {
          className: "segment-button",
          type: "button",
          "aria-pressed": String(value === option),
          onclick: () => updateDraft(repository, control, option),
        },
        [preferenceValueLabel(option)],
      ),
    ),
  );
}

function retryButton() {
  return element("button", { className: "button", type: "button", onclick: loadTargets }, [
    "Retry",
  ]);
}

function element(tag, attributes = {}, children = []) {
  const node = document.createElement(tag);
  for (const [name, value] of Object.entries(attributes)) {
    if (value == null || value === false) continue;
    if (name === "className") {
      node.className = value;
    } else if (name === "onclick") {
      node.addEventListener("click", value);
    } else if (name === "style") {
      node.setAttribute("style", value);
    } else {
      node.setAttribute(name, value === true ? "" : String(value));
    }
  }
  for (const child of children) {
    if (child == null) continue;
    node.appendChild(
      typeof child === "string" ? document.createTextNode(child) : child,
    );
  }
  return node;
}

void loadTargets();
