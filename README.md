# ifood-case
A data engineering pipeline for insights for NYC Taxi Trips Data.

This case is defined to be executed in [Databricks Platform](https://www.databricks.com/br). But the majority of its content can be executed anywhere.

## Requeriments
- AWS Credential with access to S3
- An Instance of Databricks
- Setting up your S3 credentials in Databricks Secrets (for data fetching and save)

## Setting Up
- Create a Databricks Free Edition Instance
- Download Databricks CLI with `pip install databricks-sdk`
- Authenticate with `databricks auth login --host {YOUR_INSTANCE_URL}`
- Create a secret scope with `databricks secrets create-scope aws`
- Save your secrets with:
    - `databricks secrets put-secret aws {AWS_ACCESS_KEY_ID}`
    - `databricks secrets put-secret aws {AWS_SECRET_ACCESS_KEY}`

