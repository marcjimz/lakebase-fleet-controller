# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakebase Fleet Autoscaler — Plan Task
# MAGIC
# MAGIC Reads workspace state, classifies instances, and emits task values
# MAGIC for the downstream `cleanup` and `fill` tasks.

# COMMAND ----------

import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("autoscaler.plan")

OWNER_DAB = "dab"
OWNER_PLACEHOLDER = "autoscaler-placeholder"
_PROJECTS_API = "/api/2.0/postgres/projects"

# COMMAND ----------

# Read parameters from notebook widgets
dbutils.widgets.text("real_names", "", "Real instance names (pipe-separated)")
dbutils.widgets.text("quota", "1000", "Workspace quota")

raw_real_names = dbutils.widgets.get("real_names")
quota = int(dbutils.widgets.get("quota"))

known_real_names = {n.strip() for n in raw_real_names.split("|") if n.strip()}

logger.info("Plan inputs: real_names=%s, quota=%d", known_real_names, quota)

# COMMAND ----------

# List all Lakebase instances in the workspace

from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()


def list_all_projects():
    """List ALL Lakebase projects via /api/2.0/postgres/projects."""
    projects = []
    page_token = None
    while True:
        url = _PROJECTS_API
        if page_token:
            url = f"{url}?page_token={page_token}"
        resp = ws.api_client.do("GET", url)
        for item in resp.get("projects", []):
            status = item.get("status", {})
            tags = status.get("custom_tags") or []
            tag_map = {t["key"]: t["value"] for t in tags if "key" in t}
            project_id = status.get("project_id") or item.get("name", "").removeprefix("projects/")
            projects.append({
                "name": project_id,
                "owner": tag_map.get("owner", ""),
                "creation_time": item.get("create_time", ""),
            })
        page_token = resp.get("next_page_token")
        if not page_token:
            break
    return projects


all_instances = list_all_projects()
logger.info("Found %d total projects in workspace", len(all_instances))

# COMMAND ----------

# Classify instances

real_instances = []
placeholders = []
orphans = []

for inst in all_instances:
    name = inst["name"]
    owner = inst["owner"]

    if owner == OWNER_DAB and name in known_real_names:
        real_instances.append(inst)
    elif owner == OWNER_PLACEHOLDER:
        placeholders.append(inst)
    elif owner == OWNER_DAB:
        # DAB-owned but not in our real_names list — leave it alone (safety)
        logger.info("Skipping unknown DAB instance: %s", name)
    else:
        # Not DAB, not placeholder — orphan
        orphans.append(inst)

logger.info(
    "Classification: real=%d, placeholders=%d, orphans=%d",
    len(real_instances), len(placeholders), len(orphans),
)

# COMMAND ----------

# Compute target placeholder count and what to create/delete

target_placeholders = quota - len(known_real_names)
if target_placeholders < 0:
    raise ValueError(
        f"Real ({len(known_real_names)}) exceeds quota ({quota})"
    )

current_placeholder_count = len(placeholders)

# --- Deletions: orphans + excess placeholders ---
delete_names = [inst["name"] for inst in orphans]

if current_placeholder_count > target_placeholders:
    # Sort by creation_time so we delete the oldest first
    placeholders.sort(key=lambda i: i["creation_time"])
    excess = current_placeholder_count - target_placeholders
    excess_placeholders = placeholders[:excess]
    delete_names.extend(inst["name"] for inst in excess_placeholders)
    # Remaining placeholders after excess removal
    remaining_placeholders = placeholders[excess:]
else:
    remaining_placeholders = placeholders

# --- Creations: fill gap ---
need_to_create = target_placeholders - len(remaining_placeholders)

existing_indices = set()
for inst in remaining_placeholders:
    name = inst["name"]
    if name.startswith("fleet-placeholder-"):
        try:
            existing_indices.add(int(name.split("-")[-1]))
        except ValueError:
            pass

create_names = []
idx = 0
for _ in range(max(0, need_to_create)):
    while idx in existing_indices:
        idx += 1
    create_names.append(f"fleet-placeholder-{idx:04d}")
    existing_indices.add(idx)
    idx += 1

logger.info("Plan: delete %d instances, create %d placeholders", len(delete_names), len(create_names))

# COMMAND ----------

# Emit task values for downstream tasks

delete_value = "|".join(delete_names) if delete_names else "__NONE__"

summary = {
    "quota": quota,
    "real_count": len(known_real_names),
    "target_placeholders": target_placeholders,
    "current_placeholders": current_placeholder_count,
    "orphans_found": len(orphans),
    "to_delete": len(delete_names),
    "to_create": len(create_names),
}

dbutils.jobs.taskValues.set(key="delete_names", value=delete_value)
dbutils.jobs.taskValues.set(key="summary", value=summary)

logger.info("Task values set — summary: %s", json.dumps(summary, indent=2))
logger.info("delete_names: %s", delete_value[:200])
logger.info("to_create: %d names (computed by fill task)", len(create_names))
