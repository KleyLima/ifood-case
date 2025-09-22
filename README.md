# ifood-case
A data engineering pipeline for insights for NYC Taxi Trips Data.

This case is defined to be executed in [Databricks Platform](https://www.databricks.com/br). But the majority of its content can be executed anywhere.

## Requeriments
- AWS Credential with access to S3
- An Instance of Databricks
- Setting up your S3 credentials in Databricks Secrets (for data fetching and save)

## Setting Up
- Download Databricks CLI
- Authenticate with `databricks auth login --host {YOUR_INSTANCE_URL}

