# Hosted Runtime Backends

Hosted runtime backends for Gestalt executable plugins.

Unlike the installable provider packages under `plugins/`, `auth/`, `workflow/`,
and the other runtime-managed kinds, these backends are currently linked into
`gestaltd` builds through the public hosted-runtime Go interface exposed by
`github.com/valon-technologies/gestalt/server/pluginruntime`.

That means this directory is the implementation home for runtime backends, but
not yet a separately released manifest-driven provider kind.
