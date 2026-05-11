# Provider Snapshot Infrastructure

This Terraform root manages the GitHub Actions Workload Identity binding used by
the provider snapshot publisher workflow.

The publisher service account and snapshot bucket already exist in
`valon-internal-tools-elevated`. This root intentionally manages only the
service account IAM member that lets the `valon-technologies/gestalt-providers`
GitHub repository impersonate the publisher service account.

The matching GitHub Actions workflow uses checked-in Workload Identity resource
names instead of GitHub secrets. If the service account, Workload Identity pool,
or publisher repository changes, update both this Terraform root and
`.github/workflows/publish-provider-snapshot.yml`.

## Apply

Initialize with the configured remote backend:

```bash
terraform init
terraform plan
terraform apply
```

The applying identity needs permission to read and update IAM policy on:

```text
gestalt-snapshot-pub@valon-internal-tools-elevated.iam.gserviceaccount.com
```
