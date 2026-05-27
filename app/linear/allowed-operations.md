# Linear Audited GraphQL Operations

Source: GCP `gitlab-peach-street` Cloud Run stdout audit logs, filter `jsonPayload."log.type"="audit" AND jsonPayload.provider="linear"`, window `2026-04-15T00:00:00Z` through `2026-05-16T00:00:00Z`.

This table records the audited GraphQL root fields with checked-in document-backed operations in `manifest.yaml`. Rows are sorted by observed invocation count. `graphql`, OAuth lifecycle operations, and legacy helper/MCP-only names were observed in logs but are intentionally excluded from the GraphQL root-field set.

Excluded non-operation observations: `graphql`, `connection.oauth.start`, `connection.oauth.complete`, `connection.disconnect`.

Hosted MCP tool observations: `get_issue`, `get_project`, `list_comments`, `list_issues`, `list_projects`, `list_teams`, `save_comment`, `save_issue`.

The checked-in selection sets intentionally omit Linear team-access fields (`Team.private`, `Team.securitySettings`, `Team.protected`) and issue-sharing fields (`Issue.sharedAccess`, `Issue.inheritsSharedAccess`) because those fields can require workspace features that are not enabled for every caller.

The manifest keeps full GraphQL documents in `spec.allowedOperations.<operation>.graphql.document`. The `allowedOperations` keys are Gestalt operation IDs and the documents define the upstream query or mutation shape. Hosted MCP tools remain in `allowedOperations` as explicit non-GraphQL entries so the composite GraphQL/MCP provider keeps exposing those audited MCP operations.

| Operation | Kind | Return type | Count | Risk | Selection owner | Audit source |
| --- | --- | --- | ---: | --- | --- | --- |
| `issue` | Query | `Issue` | 1582 | read | `manifest.yaml` | GCP audit logs |
| `searchIssues` | Query | `IssueSearchPayload` | 1524 | read | `manifest.yaml` | GCP audit logs |
| `issueLabels` | Query | `IssueLabelConnection` | 728 | read | `manifest.yaml` | GCP audit logs |
| `issues` | Query | `IssueConnection` | 655 | read | `manifest.yaml` | GCP audit logs |
| `projects` | Query | `ProjectConnection` | 328 | read | `manifest.yaml` | GCP audit logs |
| `viewer` | Query | `User` | 320 | read | `manifest.yaml` | GCP audit logs |
| `issueCreate` | Mutation | `IssuePayload` | 298 | write | `manifest.yaml` | GCP audit logs |
| `teams` | Query | `TeamConnection` | 292 | read | `manifest.yaml` | GCP audit logs |
| `commentCreate` | Mutation | `CommentPayload` | 279 | write | `manifest.yaml` | GCP audit logs |
| `project` | Query | `Project` | 207 | read | `manifest.yaml` | GCP audit logs |
| `comments` | Query | `CommentConnection` | 205 | read | `manifest.yaml` | GCP audit logs |
| `issueUpdate` | Mutation | `IssuePayload` | 200 | write | `manifest.yaml` | GCP audit logs |
| `initiatives` | Query | `InitiativeConnection` | 183 | read | `manifest.yaml` | GCP audit logs |
| `semanticSearch` | Query | `SemanticSearchPayload` | 167 | read | `manifest.yaml` | GCP audit logs |
| `workflowStates` | Query | `WorkflowStateConnection` | 102 | read | `manifest.yaml` | GCP audit logs |
| `fetchData` | Query | `FetchDataPayload` | 84 | read | `manifest.yaml` | GCP audit logs |
| `initiativeToProjects` | Query | `InitiativeToProjectConnection` | 78 | read | `manifest.yaml` | GCP audit logs |
| `team` | Query | `Team` | 72 | read | `manifest.yaml` | GCP audit logs |
| `searchProjects` | Query | `ProjectSearchPayload` | 67 | read | `manifest.yaml` | GCP audit logs |
| `attachments` | Query | `AttachmentConnection` | 66 | read | `manifest.yaml` | GCP audit logs |
| `issueSearch` | Query | `IssueConnection` | 41 | read | `manifest.yaml` | GCP audit logs |
| `administrableTeams` | Query | `TeamConnection` | 41 | read | `manifest.yaml` | GCP audit logs |
| `customerNeeds` | Query | `CustomerNeedConnection` | 30 | read | `manifest.yaml` | GCP audit logs |
| `issueVcsBranchSearch` | Query | `Issue` | 25 | read | `manifest.yaml` | GCP audit logs |
| `users` | Query | `UserConnection` | 22 | read | `manifest.yaml` | GCP audit logs |
| `cycles` | Query | `CycleConnection` | 20 | read | `manifest.yaml` | GCP audit logs |
| `documents` | Query | `DocumentConnection` | 15 | read | `manifest.yaml` | GCP audit logs |
| `issueBatchUpdate` | Mutation | `IssueBatchPayload` | 13 | write | `manifest.yaml` | GCP audit logs |
| `projectUpdate` | Mutation | `ProjectPayload` | 11 | write | `manifest.yaml` | GCP audit logs |
| `attachmentsForURL` | Query | `AttachmentConnection` | 11 | read | `manifest.yaml` | GCP audit logs |
| `template` | Query | `Template` | 10 | read | `manifest.yaml` | GCP audit logs |
| `teamMemberships` | Query | `TeamMembershipConnection` | 9 | read | `manifest.yaml` | GCP audit logs |
| `notifications` | Query | `NotificationConnection` | 8 | read | `manifest.yaml` | GCP audit logs |
| `issueLabel` | Query | `IssueLabel` | 8 | read | `manifest.yaml` | GCP audit logs |
| `templates` | Query | `Template` | 7 | read | `manifest.yaml` | GCP audit logs |
| `issueRelations` | Query | `IssueRelationConnection` | 7 | read | `manifest.yaml` | GCP audit logs |
| `customViews` | Query | `CustomViewConnection` | 7 | read | `manifest.yaml` | GCP audit logs |
| `customView` | Query | `CustomView` | 7 | read | `manifest.yaml` | GCP audit logs |
| `user` | Query | `User` | 6 | read | `manifest.yaml` | GCP audit logs |
| `projectMilestones` | Query | `ProjectMilestoneConnection` | 6 | read | `manifest.yaml` | GCP audit logs |
| `issueDelete` | Mutation | `IssueArchivePayload` | 6 | write | `manifest.yaml` | GCP audit logs |
| `fileUpload` | Mutation | `UploadPayload` | 6 | write | `manifest.yaml` | GCP audit logs |
| `favorites` | Query | `FavoriteConnection` | 6 | read | `manifest.yaml` | GCP audit logs |
| `commentDelete` | Mutation | `DeletePayload` | 6 | write | `manifest.yaml` | GCP audit logs |
| `searchDocuments` | Query | `DocumentSearchPayload` | 5 | read | `manifest.yaml` | GCP audit logs |
| `projectMilestone` | Query | `ProjectMilestone` | 5 | read | `manifest.yaml` | GCP audit logs |
| `organization` | Query | `Organization` | 5 | read | `manifest.yaml` | GCP audit logs |
| `issueRelationCreate` | Mutation | `IssueRelationPayload` | 5 | write | `manifest.yaml` | GCP audit logs |
| `attachmentCreate` | Mutation | `AttachmentPayload` | 5 | write | `manifest.yaml` | GCP audit logs |
| `projectUpdates` | Query | `ProjectUpdateConnection` | 4 | read | `manifest.yaml` | GCP audit logs |
| `issueArchive` | Mutation | `IssueArchivePayload` | 4 | write | `manifest.yaml` | GCP audit logs |
| `projectLabels` | Query | `ProjectLabelConnection` | 3 | read | `manifest.yaml` | GCP audit logs |
| `issuePriorityValues` | Query | `IssuePriorityValue` | 3 | read | `manifest.yaml` | GCP audit logs |
| `issueLabelUpdate` | Mutation | `IssueLabelPayload` | 3 | write | `manifest.yaml` | GCP audit logs |
| `issueLabelCreate` | Mutation | `IssueLabelPayload` | 3 | write | `manifest.yaml` | GCP audit logs |
| `attachmentDelete` | Mutation | `DeletePayload` | 3 | write | `manifest.yaml` | GCP audit logs |
| `archivedTeams` | Query | `Team` | 3 | read | `manifest.yaml` | GCP audit logs |
| `agentSessions` | Query | `AgentSessionConnection` | 3 | read | `manifest.yaml` | GCP audit logs |
| `userSettings` | Query | `UserSettings` | 2 | read | `manifest.yaml` | GCP audit logs |
| `projectStatuses` | Query | `ProjectStatusConnection` | 2 | read | `manifest.yaml` | GCP audit logs |
| `initiativeToProjectCreate` | Mutation | `InitiativeToProjectPayload` | 2 | write | `manifest.yaml` | GCP audit logs |
| `initiative` | Query | `Initiative` | 2 | read | `manifest.yaml` | GCP audit logs |
| `customViewCreate` | Mutation | `CustomViewPayload` | 2 | write | `manifest.yaml` | GCP audit logs |
| `commentUpdate` | Mutation | `CommentPayload` | 2 | write | `manifest.yaml` | GCP audit logs |
| `availableUsers` | Query | `AuthResolverResponse` | 2 | read | `manifest.yaml` | GCP audit logs |
| `attachmentLinkGitHubPR` | Mutation | `AttachmentPayload` | 2 | write | `manifest.yaml` | GCP audit logs |
| `attachment` | Query | `Attachment` | 2 | read | `manifest.yaml` | GCP audit logs |
| `workflowState` | Query | `WorkflowState` | 1 | read | `manifest.yaml` | GCP audit logs |
| `viewPreferencesCreate` | Mutation | `ViewPreferencesPayload` | 1 | write | `manifest.yaml` | GCP audit logs |
| `templatesForIntegration` | Query | `Template` | 1 | read | `manifest.yaml` | GCP audit logs |
| `releaseSearch` | Query | `Release` | 1 | read | `manifest.yaml` | GCP audit logs |
| `projectRelations` | Query | `ProjectRelationConnection` | 1 | read | `manifest.yaml` | GCP audit logs |
| `projectDelete` | Mutation | `ProjectArchivePayload` | 1 | write | `manifest.yaml` | GCP audit logs |
| `issueFigmaFileKeySearch` | Query | `IssueConnection` | 1 | read | `manifest.yaml` | GCP audit logs |
| `issueDescriptionUpdateFromFront` | Mutation | `IssuePayload` | 1 | write | `manifest.yaml` | GCP audit logs |
| `cycle` | Query | `Cycle` | 1 | read | `manifest.yaml` | GCP audit logs |
| `customers` | Query | `CustomerConnection` | 1 | read | `manifest.yaml` | GCP audit logs |
| `comment` | Query | `Comment` | 1 | read | `manifest.yaml` | GCP audit logs |
| `attachmentSources` | Query | `AttachmentSourcesPayload` | 1 | read | `manifest.yaml` | GCP audit logs |
| `attachmentLinkURL` | Mutation | `AttachmentPayload` | 1 | write | `manifest.yaml` | GCP audit logs |
