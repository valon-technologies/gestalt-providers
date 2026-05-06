# GitHub UI

Mounted UI for GitHub integration settings.

Example deployment config:

```yaml
providers:
  ui:
    github:
      path: /github
      source:
        repo: valon
        package: github.com/valon-technologies/gestalt-providers/ui/github
        version: 0.0.1-alpha.1
```

The UI calls the GitHub provider's `actionPreferences.*` operations through
the same-origin Gestalt API.
