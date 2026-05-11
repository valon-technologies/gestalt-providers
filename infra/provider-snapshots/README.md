# Provider Snapshot Infrastructure

This Terraform root manages the GitHub Actions Workload Identity binding used by
the provider snapshot publisher workflow.

The publisher service account and snapshot bucket already exist in
`valon-internal-tools-elevated`. This root intentionally manages only the
service account IAM member that lets the `valon-technologies/gestalt-providers`
GitHub repository impersonate the publisher service account.

## Apply

Pick the approved remote state bucket/prefix for elevated internal-tools
infrastructure, then initialize with backend config:

```bash
terraform init \
  -backend-config=bucket=<terraform-state-bucket> \
  -backend-config=prefix=gestalt-providers/provider-snapshots
terraform plan
terraform apply
```

The applying identity needs permission to read and update IAM policy on:

```text
gestalt-snapshot-pub@valon-internal-tools-elevated.iam.gserviceaccount.com
```

