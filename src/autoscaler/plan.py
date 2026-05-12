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
dbutils.widgets.text("enabled", "true", "Enable autoscaler")
dbutils.widgets.text("placeholders", "true", "Fill with placeholders")

raw_real_names = dbutils.widgets.get("real_names")
quota = int(dbutils.widgets.get("quota"))
enabled = dbutils.widgets.get("enabled").lower() == "true"
placeholders_enabled = dbutils.widgets.get("placeholders").lower() == "true"

known_real_names = {n.strip() for n in raw_real_names.split("|") if n.strip()}

logger.info("Plan inputs: real_names=%s, quota=%d, enabled=%s, placeholders=%s",
            known_real_names, quota, enabled, placeholders_enabled)

# COMMAND ----------

# Early exit if disabled

if not enabled:
    logger.info("Autoscaler DISABLED — emitting no-op sentinels")
    dbutils.jobs.taskValues.set(key="delete_names", value="__NONE__")
    dbutils.jobs.taskValues.set(key="fill_slices", value=["__SKIP__"])
    dbutils.jobs.taskValues.set(key="summary", value={
        "enabled": False, "placeholders": False,
        "quota": quota, "to_delete": 0, "to_create": 0,
    })

# COMMAND ----------

if enabled:
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

    # Compute deletions and fill slices based on placeholders flag

    target_placeholders = quota - len(known_real_names)
    if target_placeholders < 0:
        raise ValueError(
            f"Real ({len(known_real_names)}) exceeds quota ({quota})"
        )

    current_placeholder_count = len(placeholders)

    # --- Deletions: always delete orphans ---
    delete_names = [inst["name"] for inst in orphans]

    if placeholders_enabled:
        # Also delete excess placeholders when managing placeholders
        if current_placeholder_count > target_placeholders:
            placeholders.sort(key=lambda i: i["creation_time"])
            excess = current_placeholder_count - target_placeholders
            delete_names.extend(inst["name"] for inst in placeholders[:excess])
            remaining_placeholders = placeholders[excess:]
        else:
            remaining_placeholders = placeholders

        need_to_create = target_placeholders - len(remaining_placeholders)
    else:
        need_to_create = 0

    # --- Fill slices ---
    if placeholders_enabled and need_to_create > 0:
        fill_slices = list(range(10))  # [0, 1, ..., 9]
    else:
        fill_slices = ["__SKIP__"]

    logger.info("Plan: delete %d instances, need_to_create %d, fill_slices=%s",
                len(delete_names), need_to_create, fill_slices)

    # COMMAND ----------

    # Emit task values for downstream tasks

    delete_value = "|".join(delete_names) if delete_names else "__NONE__"

    summary = {
        "enabled": True,
        "placeholders": placeholders_enabled,
        "quota": quota,
        "real_count": len(known_real_names),
        "target_placeholders": target_placeholders,
        "current_placeholders": current_placeholder_count,
        "orphans_found": len(orphans),
        "to_delete": len(delete_names),
        "to_create": need_to_create,
        "fill_slices": fill_slices,
    }

    dbutils.jobs.taskValues.set(key="delete_names", value=delete_value)
    dbutils.jobs.taskValues.set(key="fill_slices", value=fill_slices)
    dbutils.jobs.taskValues.set(key="summary", value=summary)

    logger.info("Task values set — summary: %s", json.dumps(summary, indent=2))
    logger.info("delete_names: %s", delete_value[:200])
