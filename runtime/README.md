# Runtime Providers

Runtime providers for Gestalt hosted executable plugins.

Packages under this directory are first-class manifest-driven `kind: runtime`
providers. `gestaltd` prepares and launches them the same way it handles other
source-backed provider kinds, while plugins can select them through
`plugins.<name>.runtime.provider`.
