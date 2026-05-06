"use client";

import { useEffect, useMemo, useState } from "react";
import {
  APIError,
  deleteGitHubActionPreference,
  GitHubActionPreferenceControl,
  GitHubActionPreferenceField,
  GitHubActionPreferenceRepository,
  GitHubActionPreferenceTargets,
  isAPIErrorStatus,
  listGitHubActionPreferenceTargets,
  setGitHubActionPreference,
} from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import Button from "./Button";

type PreferenceValue = boolean | null;
type PreferenceDrafts = Record<string, PreferenceValue>;
type PreferenceIdentityKind = "external_subject_id" | "subject_id";

const PREFERENCE_FIELDS: GitHubActionPreferenceField[] = [
  "allow_code_review_comments",
  "allow_self_fix",
];

function controlKey(
  repository: string,
  control: GitHubActionPreferenceControl,
): string {
  return `${repository}\u0000${control.policy_id}\u0000${control.field}`;
}

function controlIdentityKind(
  control: GitHubActionPreferenceControl,
): PreferenceIdentityKind {
  return control.identity_kind === "subject_id" ? "subject_id" : "external_subject_id";
}

function preferenceValueLabel(value: PreferenceValue): string {
  if (value === true) return "On";
  if (value === false) return "Off";
  return "Default";
}

function controlValue(
  repository: string,
  control: GitHubActionPreferenceControl,
  drafts: PreferenceDrafts,
): PreferenceValue {
  const key = controlKey(repository, control);
  return key in drafts ? drafts[key] : control.stored;
}

function isFallbackError(error: unknown): boolean {
  return (
    isAPIErrorStatus(error, 404) ||
    isAPIErrorStatus(error, 412) ||
    (error instanceof APIError && error.status === 405)
  );
}

export default function GitHubActionPreferencesPanel() {
  const [targets, setTargets] = useState<GitHubActionPreferenceTargets | null>(null);
  const [selectedRepository, setSelectedRepository] = useState("");
  const [drafts, setDrafts] = useState<PreferenceDrafts>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [hidden, setHidden] = useState(false);
  const [error, setError] = useState("");
  const [saved, setSaved] = useState(false);

  async function loadTargets() {
    setLoading(true);
    setError("");
    try {
      const nextTargets = await listGitHubActionPreferenceTargets();
      setTargets(nextTargets);
      setHidden(nextTargets.repositories.length === 0);
      setDrafts({});
      setSelectedRepository((current) => {
        if (
          current &&
          nextTargets.repositories.some((repo) => repo.repository === current)
        ) {
          return current;
        }
        return nextTargets.repositories[0]?.repository ?? "";
      });
    } catch (err) {
      if (isFallbackError(err)) {
        setHidden(true);
      } else {
        setError(err instanceof Error ? err.message : "Unable to load preferences");
      }
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadTargets();
  }, []);

  const selected = useMemo(() => {
    return (
      targets?.repositories.find(
        (repository) => repository.repository === selectedRepository,
      ) ?? targets?.repositories[0]
    );
  }, [selectedRepository, targets]);

  const dirty = useMemo(() => {
    if (!selected) return false;
    return selected.controls.some((control) => {
      const key = controlKey(selected.repository, control);
      return key in drafts && drafts[key] !== control.stored;
    });
  }, [drafts, selected]);

  function updateDraft(
    repository: string,
    control: GitHubActionPreferenceControl,
    value: PreferenceValue,
  ) {
    const key = controlKey(repository, control);
    setSaved(false);
    setDrafts((current) => {
      if (value === control.stored) {
        const next = { ...current };
        delete next[key];
        return next;
      }
      return { ...current, [key]: value };
    });
  }

  async function save() {
    if (!selected || !dirty) return;
    setSaving(true);
    setError("");
    setSaved(false);
    try {
      const controlsByPolicy = new Map<string, GitHubActionPreferenceControl[]>();
      for (const control of selected.controls) {
        const groupKey = `${control.policy_id}\u0000${controlIdentityKind(control)}`;
        const controls = controlsByPolicy.get(groupKey) ?? [];
        controls.push(control);
        controlsByPolicy.set(groupKey, controls);
      }

      for (const [groupKey, controls] of controlsByPolicy) {
        const [policyID, identityKind] = groupKey.split("\u0000") as [
          string,
          PreferenceIdentityKind,
        ];
        const values: Partial<Record<GitHubActionPreferenceField, PreferenceValue>> =
          {};
        let policyDirty = false;
        for (const control of controls) {
          const value = controlValue(selected.repository, control, drafts);
          values[control.field] = value;
          policyDirty = policyDirty || value !== control.stored;
        }
        if (!policyDirty) continue;

        const allDefault = PREFERENCE_FIELDS.every((field) => values[field] == null);
        if (allDefault) {
          await deleteGitHubActionPreference({
            repository: selected.repository,
            policy_id: policyID,
            identity_kind: identityKind,
          });
        } else {
          await setGitHubActionPreference({
            repository: selected.repository,
            policy_id: policyID,
            identity_kind: identityKind,
            allow_code_review_comments:
              values.allow_code_review_comments ?? null,
            allow_self_fix: values.allow_self_fix ?? null,
          });
        }
      }
      setSaved(true);
      await loadTargets();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to save preferences");
    } finally {
      setSaving(false);
    }
  }

  if (hidden) return null;

  return (
    <section className="mt-5 border-t border-alpha pt-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-semibold text-primary">GitHub Bot Actions</h3>
          {selected ? (
            <p className="mt-1 text-xs text-muted">{selected.repository}</p>
          ) : null}
        </div>
        <Button
          type="button"
          variant="secondary"
          className="px-3 py-1.5 text-xs"
          onClick={save}
          disabled={!dirty || loading || saving}
        >
          {saving ? "Saving..." : "Save"}
        </Button>
      </div>

      {loading ? <p className="mt-3 text-sm text-muted">Loading...</p> : null}
      {error ? <p className="mt-3 text-sm text-ember-500">{error}</p> : null}
      {saved ? <p className="mt-3 text-sm text-grove-600">Saved</p> : null}

      {!loading && selected ? (
        <div className="mt-4 space-y-4">
          <RepositorySelector
            repositories={targets?.repositories ?? []}
            selectedRepository={selected.repository}
            onSelect={(repository) => {
              setSaved(false);
              setSelectedRepository(repository);
            }}
          />
          <div className="space-y-3">
            {selected.controls.map((control) => (
              <PreferenceControlRow
                key={control.id}
                repository={selected.repository}
                control={control}
                value={controlValue(selected.repository, control, drafts)}
                onChange={(value) => updateDraft(selected.repository, control, value)}
              />
            ))}
          </div>
        </div>
      ) : null}
    </section>
  );
}

function RepositorySelector({
  repositories,
  selectedRepository,
  onSelect,
}: {
  repositories: GitHubActionPreferenceRepository[];
  selectedRepository: string;
  onSelect: (repository: string) => void;
}) {
  if (repositories.length <= 1) return null;
  return (
    <label className="block text-xs font-medium text-muted">
      Repository
      <select
        className={`mt-1.5 ${INPUT_CLASSES}`}
        value={selectedRepository}
        onChange={(event) => onSelect(event.target.value)}
      >
        {repositories.map((repository) => (
          <option key={repository.repository} value={repository.repository}>
            {repository.repository}
          </option>
        ))}
      </select>
    </label>
  );
}

function PreferenceControlRow({
  repository,
  control,
  value,
  onChange,
}: {
  repository: string;
  control: GitHubActionPreferenceControl;
  value: PreferenceValue;
  onChange: (value: PreferenceValue) => void;
}) {
  return (
    <div className="rounded-md border border-alpha px-3 py-3">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <div className="text-sm font-medium text-primary">{control.label}</div>
          {control.description ? (
            <p className="mt-1 text-xs text-muted">{control.description}</p>
          ) : null}
          <p className="mt-1 text-xs text-faint">
            Effective: {preferenceValueLabel(control.effective)}
          </p>
        </div>
        <div
          className="grid shrink-0 grid-cols-3 rounded-md border border-alpha bg-base-100 p-0.5 dark:bg-surface-raised"
          role="group"
          aria-label={`${control.label} for ${repository}`}
        >
          {[null, true, false].map((option) => {
            const selected = value === option;
            return (
              <button
                key={String(option)}
                type="button"
                className={`min-w-16 rounded px-2.5 py-1 text-xs font-medium transition-colors duration-150 ${
                  selected
                    ? "bg-base-white text-primary shadow-sm dark:bg-base-800"
                    : "text-muted hover:text-primary"
                }`}
                aria-pressed={selected}
                onClick={() => onChange(option)}
              >
                {preferenceValueLabel(option)}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
