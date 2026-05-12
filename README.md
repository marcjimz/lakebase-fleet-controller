![Lakebase Fleet Controller](docs/header.png)

# Lakebase Fleet Controller

> **Accelerator — use at your own risk.** This has not been validated for production use. Review all code and adapt it to your environment before deploying.

> **Do not use the same workspace across multiple promotion stages (DEV/QA/STG/PROD).** This has not been tested and is not recommended.

A single Databricks Asset Bundle that manages a fleet of Lakebase projects. You edit `databricks.yml` to add or remove projects; CI handles the rest. An autoscaler notebook runs on a schedule to enforce the desired state — cleaning up orphans and optionally filling remaining quota with placeholders.

## Parameters

The autoscaler job accepts four parameters. CI passes them via `--params`; scheduled runs use the defaults from `databricks.yml`.

| Parameter | Default | Description |
|---|---|---|
| `enabled` | `"true"` | Master switch. When `true`, the autoscaler runs normally (list, classify, delete, fill). When `false`, the notebook exits immediately and does nothing. |
| `placeholders` | `"false"` | Controls placeholder lifecycle. When `true`, the autoscaler fills every unused slot up to `quota` with `fleet-placeholder-NNNN` projects. When `false`, all existing placeholders are **deleted** and no new ones are created. Orphan cleanup still runs regardless. |
| `quota` | `1000` | Maximum number of Lakebase projects allowed in the workspace. `target_placeholders = quota - real_count`. |
| `real_names` | `""` | Pipe-separated list of DAB-managed project IDs (e.g. `"my-project\|other-project"`). These are protected from deletion. CI extracts this automatically from the bundle's `postgres_projects` resources. |

## Setup

### 1. Create a service principal

Create a service principal in your Databricks workspace for CI authentication:

```bash
# Create the SP
databricks service-principals create --display-name "lakebase-fleet-controller-ci" -o json

# Note the applicationId from the output — this is your SP_CLIENT_ID

# Generate an OAuth secret (note the id from the create output)
databricks api post /api/2.0/accounts/servicePrincipals/<SP_ID>/credentials/secrets

# Note the secret from the output — this is your SP_CLIENT_SECRET

# Add the SP to the admins group
# Option A: SCIM API (non-AIM workspaces)
databricks groups list -o json  # find the admins group ID
databricks api patch /api/2.0/preview/scim/v2/Groups/<ADMINS_GROUP_ID> --json '{
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
  "Operations": [{"op": "add", "path": "members", "value": [{"value": "<SP_ID>"}]}]
}'

# Option B: Account-level AIM (Azure / AIM-enrolled workspaces)
# databricks account groups list --profile ACCOUNT -o json  # find the admins group ID
# databricks account groups patch <ADMINS_GROUP_ID> --profile ACCOUNT --json '{
#   "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
#   "Operations": [{"op": "add", "path": "members", "value": [{"value": "<SP_ID>"}]}]
# }'
```

### 2. Create a GitHub environment

In your repo settings, create a **DEV** environment with these secrets:

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | Your workspace URL (e.g. `https://myworkspace.cloud.databricks.com`) |
| `SP_CLIENT_ID` | Service principal `applicationId` from step 1 |
| `SP_CLIENT_SECRET` | OAuth `secret` from step 1 |

### 3. Configure the bundle

In `databricks.yml`, set your workspace host under `targets.dev`.

### 4. Add a project

Uncomment the `postgres_projects` section in `databricks.yml` and add a resource block:

```yaml
  postgres_projects:
    my_project:
      project_id: my-project
      display_name: my-project
      pg_version: 17
      custom_tags:
        - key: owner
          value: dab
        - key: managed_by
          value: lakebase-fleet-controller
```

Then update the `real_names` default in the job parameters to include the new project ID (pipe-separated if multiple).

Push to `main`. CI deploys the project and runs the autoscaler.

### 5. Remove a project

Delete the resource block from `databricks.yml` and remove the project ID from `real_names`. Push to `main`. The autoscaler will classify the project as an orphan and delete it on the next run.

### 6. Enable promotion stages

Uncomment the `qa`, `stg`, and/or `prod` targets in `databricks.yml` and the matching jobs in `.github/workflows/deploy.yml`. Create a GitHub environment for each with its own secrets. Stages depend on each other: DEV -> QA -> STG -> PROD.
