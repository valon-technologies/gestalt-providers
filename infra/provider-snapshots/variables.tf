variable "elevated_project_id" {
  description = "GCP project containing the provider snapshot publisher service account."
  type        = string
  default     = "valon-internal-tools-elevated"
}

variable "snapshot_publisher_service_account_email" {
  description = "Service account used by GitHub Actions to publish provider snapshots."
  type        = string
  default     = "gestalt-snapshot-pub@valon-internal-tools-elevated.iam.gserviceaccount.com"
}

variable "github_actions_project_number" {
  description = "Project number containing the shared GitHub Actions workload identity pool."
  type        = string
  default     = "508160571681"
}

variable "github_actions_pool_id" {
  description = "Workload identity pool id used by GitHub Actions."
  type        = string
  default     = "github-pool"
}

variable "github_repository" {
  description = "GitHub repository allowed to impersonate the snapshot publisher service account."
  type        = string
  default     = "valon-technologies/gestalt-providers"
}

