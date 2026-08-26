# Agent instructions — gestalt-providers

This is a public, tenant-neutral provider repository. Product consoles and
homepages that consume these providers are maintained separately; do not add
deployment-specific homepage or product UI here.

Keep provider packages independently usable and tenant-neutral. Deployment
branding, licensed fonts, and deployment-only configuration belong in the
deployment or product repository, not in provider source.

For provider changes, keep manifests, generated registry metadata, tests, and
release configuration consistent. Run the repository's focused checks and use
the `/pr-review` skill when reviewing a pull request.
