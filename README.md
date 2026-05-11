![Lakebase Fleet Controller](docs/header.png)

# Lakebase Fleet Controller

> **Accelerator — use at your own risk.** This has not been validated for production use. Review all code and adapt it to your environment before deploying.

> **Do not use the same workspace across multiple promotion stages (DEV/QA/STG/PROD).** This has not been tested and is not recommended.

A single Databricks Asset Bundle that manages a fleet of Lakebase projects. You edit `databricks.yml` to add or remove projects; CI handles the rest.

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

# Add the SP to the admins group (get the group ID first)
databricks groups list -o json  # find the admins group ID
databricks api patch /api/2.0/preview/scim/v2/Groups/<ADMINS_GROUP_ID> --json '{
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
  "Operations": [{"op": "add", "path": "members", "value": [{"value": "<SP_ID>"}]}]
}'
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

Add a `database_instances` resource block to `databricks.yml`:

```yaml
    lakebase_my_project:
      name: my-project
      capacity: CU_1    # CU_1, CU_2, CU_4, or CU_8
      custom_tags:
        - key: owner
          value: dab
        - key: managed_by
          value: lakebase-fleet-controller
      lifecycle:
        prevent_destroy: true
```

Push to `main`. CI deploys the project and fills remaining quota with placeholders.

### 5. Remove a project

Two-PR process (by design):

1. First PR: set `prevent_destroy: false` on the resource
2. Second PR: delete the resource block

### 6. Enable promotion stages

Uncomment the `qa`, `stg`, and/or `prod` targets in `databricks.yml` and the matching jobs in `.github/workflows/deploy.yml`. Create a GitHub environment for each with its own secrets. Stages depend on each other: DEV -> QA -> STG -> PROD.
