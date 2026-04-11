# Integration Plugins

Integration plugin packages for [Gestalt](https://github.com/valon-technologies/gestalt).
Each plugin connects Gestalt to an external service or API, exposing its
capabilities as operations that `gestaltd` can invoke at runtime.

Plugins range from fully declarative (manifest-only) to source-backed
implementations in Go or Python. See the
[provider development guide](https://gestaltd.ai/providers) for writing custom
plugins and the
[manifest reference](https://gestaltd.ai/reference/plugin-manifests) for the
manifest schema.

## Available Plugins

| Plugin | Version | Description |
|--------|---------|-------------|
| [Ashby](ashby/) | `0.0.1-alpha.9` | Candidates, applications, jobs, offers, interviews, departments, locations, users, and reports |
| [BigQuery](bigquery/) | `0.0.1-alpha.11` | Google BigQuery data warehouse |
| [ClickHouse](clickhouse/) | `0.0.1-alpha.8` | Query and manage analytical databases |
| [Confluence Cloud](confluence/) | `0.0.1-alpha.9` | Atlassian Confluence Cloud pages, spaces, and content search |
| [Datadog](datadog/) | `0.0.1-alpha.8` | Manage dashboards, monitors, incidents, logs, and RUM |
| [dbt Cloud](dbt_cloud/) | `0.0.1-alpha.8` | Manage accounts, projects, jobs, and runs |
| [Extend](extend/) | `0.0.1-alpha.8` | Document processing and extraction with Extend |
| [Figma](figma/) | `0.0.1-alpha.8` | Access files, components, comments, and team projects |
| [GitHub](github/) | `0.0.1-alpha.8` | Repository, issue, pull request, workflow, and code search operations |
| [GitLab](gitlab/) | `0.0.1-alpha.8` | Repository, issue, merge request, and pipeline operations |
| [Gmail](gmail/) | `0.0.1-alpha.12` | Read, send, and manage Gmail messages, threads, drafts, and labels |
| [Google Calendar](google_calendar/) | `0.0.1-alpha.10` | Read and manage Google calendars and events |
| [Google Docs](google_docs/) | `0.0.1-alpha.9` | Read, create, and edit Google Docs documents |
| [Google Drive](google_drive/) | `0.0.1-alpha.8` | Read, create, update, and share files in Google Drive |
| [Google Sheets](google_sheets/) | `0.0.1-alpha.8` | Read and update Google Sheets spreadsheets and values |
| [Google Slides](google_slides/) | `0.0.1-alpha.8` | Create and update Google Slides presentations |
| [Hex](hex/) | `0.0.1-alpha.8` | Manage Hex projects, runs, and cells |
| [HTTPBin](httpbin/) | `0.0.1-alpha.1` | HTTP request and response testing service |
| [incident.io](incident_io/) | `0.0.1-alpha.8` | Manage incidents, schedules, users, severities, and statuses |
| [Intercom](intercom/) | `0.0.1-alpha.9` | Read and update contacts, companies, conversations, and notes |
| [Jira Cloud](jira/) | `0.0.1-alpha.8` | Atlassian Jira Cloud project and issue management |
| [LaunchDarkly](launchdarkly/) | `0.0.1-alpha.8` | Manage feature flags and targeting rules |
| [Linear](linear/) | `0.0.1-alpha.8` | Manage issues, projects, and teams |
| [Modern Treasury](modern_treasury/) | `0.0.1-alpha.9` | Create payment orders, manage external accounts, and inspect treasury activity |
| [Notion](notion/) | `0.0.1-alpha.9` | Notion REST operations plus the official Notion MCP tool surface |
| [PagerDuty](pagerduty/) | `0.0.1-alpha.8` | Manage incidents, services, and on-call schedules |
| [Ramp](ramp/) | `0.0.1-alpha.8` | Manage corporate cards, transactions, and reimbursements |
| [Rippling](rippling/) | `0.0.1-alpha.10` | Access company, employee, org structure, identity, leave, and time data |
| [Slack](slack/) | `0.0.1-alpha.13` | Send messages, search conversations, and manage channels |
