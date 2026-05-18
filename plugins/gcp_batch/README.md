# GCP Batch

Google Cloud Batch job management through the Batch REST API.

This plugin is intentionally narrow. It exposes the job operations needed by
callers that should submit and poll Batch jobs with their own Google OAuth
credentials.

Requested scope:

- `https://www.googleapis.com/auth/cloud-platform`
