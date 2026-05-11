output "snapshot_publisher_github_actions_member" {
  description = "PrincipalSet member allowed to impersonate the snapshot publisher service account."
  value       = google_service_account_iam_member.snapshot_publisher_github_actions.member
}

output "snapshot_publisher_service_account_id" {
  description = "Fully qualified publisher service account resource id."
  value       = google_service_account_iam_member.snapshot_publisher_github_actions.service_account_id
}

