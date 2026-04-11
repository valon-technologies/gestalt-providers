# Datastore Providers

Datastore providers for [Gestalt](https://github.com/valon-technologies/gestalt).
Each provider implements the IndexedDB storage interface, allowing `gestaltd` to
persist state across a variety of database backends.

See the [configuration guide](https://gestaltd.ai/configuration) for setting up
datastores and the
[provider development guide](https://gestaltd.ai/providers) for writing custom
datastore providers.

## Available Providers

| Provider | Version | Description |
|----------|---------|-------------|
| [DynamoDB](dynamodb/) | `0.0.1-alpha.2` | Amazon DynamoDB datastore provider |
| [MongoDB](mongodb/) | `0.0.1-alpha.2` | MongoDB datastore provider |
| [RelationalDB](relationaldb/) | `0.0.1-alpha.2` | PostgreSQL, MySQL, SQLite, and SQL Server |
