terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }

  backend "gcs" {
    bucket = "toolshed-terraform-state"
    prefix = "gestalt-providers/provider-snapshots"
  }
}

provider "google" {
  project = var.elevated_project_id
}

locals {
  snapshot_publisher_service_account_id = "projects/${var.elevated_project_id}/serviceAccounts/${var.snapshot_publisher_service_account_email}"
  github_actions_repository_member      = "principalSet://iam.googleapis.com/projects/${var.github_actions_project_number}/locations/global/workloadIdentityPools/${var.github_actions_pool_id}/attribute.repository/${var.github_repository}"
}

resource "google_service_account_iam_member" "snapshot_publisher_github_actions" {
  service_account_id = local.snapshot_publisher_service_account_id
  role               = "roles/iam.workloadIdentityUser"
  member             = local.github_actions_repository_member
}
