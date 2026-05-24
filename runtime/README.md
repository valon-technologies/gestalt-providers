# Runtime Providers

Runtime providers for Gestalt hosted executable apps and agent providers.

Packages under this directory are first-class manifest-driven `kind: runtime`
providers. `gestaltd` prepares and launches them the same way it handles other
source-backed provider kinds, while executable apps and agent providers can
select them through `apps.<name>.execution.mode: hosted` or
`providers.agent.<name>.execution.mode: hosted`, with
`execution.runtime.provider` when they need to override the server default.
