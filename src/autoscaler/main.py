# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakebase Fleet Autoscaler
# MAGIC
# MAGIC Single notebook that manages the full lifecycle:
# MAGIC 1. List all projects, classify as real / placeholder / orphan
# MAGIC 2. Delete orphans + excess placeholders
# MAGIC 3. Fill missing placeholder slots (50 concurrent threads)

# COMMAND ----------

import concurrent.futures
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("autoscaler")

OWNER_DAB = "dab"
OWNER_PLACEHOLDER = "autoscaler-placeholder"
_PROJECTS_API = "/api/2.0/postgres/projects"

# COMMAND ----------

# ── Parameters ───────────────────────────────────────────────────────────────

dbutils.widgets.text("enabled", "true", "Enable autoscaler")
dbutils.widgets.text("placeholders", "true", "Fill with placeholders")
dbutils.widgets.text("quota", "1000", "Workspace quota")
dbutils.widgets.text("real_names", "", "Real instance names (pipe-separated)")
dbutils.widgets.text("contact_emails", "", "Contact emails (comma-separated)")

enabled = dbutils.widgets.get("enabled").lower() == "true"
placeholders_enabled = dbutils.widgets.get("placeholders").lower() == "true"
quota = int(dbutils.widgets.get("quota"))
raw_real_names = dbutils.widgets.get("real_names")
known_real_names = {n.strip() for n in raw_real_names.split("|") if n.strip()}
contact_emails = [e.strip() for e in dbutils.widgets.get("contact_emails").split(",") if e.strip()]

logger.info("Parameters: enabled=%s, placeholders=%s, quota=%d, real_names=%s, contact_emails=%s",
            enabled, placeholders_enabled, quota, known_real_names, contact_emails)

if not enabled:
    logger.info("Autoscaler DISABLED — exiting")
    dbutils.notebook.exit("DISABLED")

# COMMAND ----------

# ── API client ───────────────────────────────────────────────────────────────

from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()


def list_all_projects():
    """List ALL Lakebase projects via paginated API."""
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


def create_project(project_id, display_name, custom_tags=None):
    """Create a Lakebase project. project_id is a query parameter (AIP-style)."""
    body = {"spec": {"display_name": display_name}}
    if custom_tags:
        body["spec"]["custom_tags"] = custom_tags
    return ws.api_client.do("POST", f"{_PROJECTS_API}?project_id={project_id}", body=body)


def delete_project(project_id):
    ws.api_client.do("DELETE", f"{_PROJECTS_API}/{project_id}")


def suspend_endpoint(project_id):
    """Disable the primary endpoint so compute scales to zero."""
    url = (f"{_PROJECTS_API}/{project_id}/branches/production"
           f"/endpoints/primary?update_mask=spec.disabled")
    ws.api_client.do("PATCH", url, body={"spec": {"disabled": True}})

# COMMAND ----------

# ── Step 1: List & classify ──────────────────────────────────────────────────

all_projects = list_all_projects()
logger.info("Found %d total projects", len(all_projects))

real_instances = []
placeholders = []
orphans = []

for proj in all_projects:
    name = proj["name"]
    owner = proj["owner"]

    if owner == OWNER_DAB and name in known_real_names:
        real_instances.append(proj)
    elif owner == OWNER_PLACEHOLDER:
        placeholders.append(proj)
    elif owner == OWNER_DAB:
        # DAB-owned but not in our real_names list — leave it alone
        logger.info("Skipping unknown DAB instance: %s", name)
    else:
        orphans.append(proj)

logger.info("Classification: real=%d, placeholders=%d, orphans=%d",
            len(real_instances), len(placeholders), len(orphans))

# COMMAND ----------

# ── Step 2: Delete orphans + excess placeholders ─────────────────────────────

delete_names = [inst["name"] for inst in orphans]

target_placeholders = quota - len(known_real_names)
if target_placeholders < 0:
    raise ValueError(f"Real ({len(known_real_names)}) exceeds quota ({quota})")

if not placeholders_enabled:
    # Placeholders disabled — delete ALL existing placeholders
    delete_names.extend(inst["name"] for inst in placeholders)
    logger.info("Placeholders disabled — marking all %d for deletion", len(placeholders))
    placeholders = []
elif len(placeholders) > target_placeholders:
    # Trim excess placeholders
    placeholders.sort(key=lambda i: i["creation_time"])
    excess = len(placeholders) - target_placeholders
    delete_names.extend(inst["name"] for inst in placeholders[:excess])
    placeholders = placeholders[excess:]

deleted = 0
protected_branch_failures = []
for name in delete_names:
    try:
        logger.info("Deleting: %s", name)
        delete_project(name)
        deleted += 1
    except Exception as exc:
        err = str(exc)
        if "protected branches" in err or "FAILED_PRECONDITION" in err:
            logger.error("FAILED to delete %s — has protected branches", name)
            protected_branch_failures.append(name)
        elif "404" in err or "NOT_FOUND" in err:
            logger.info("%s already gone, skipping", name)
        else:
            raise

logger.info("Cleanup done: deleted=%d, protected_branch_failures=%d (of %d targeted)",
            deleted, len(protected_branch_failures), len(delete_names))

if protected_branch_failures:
    msg = (
        f"Cannot delete {len(protected_branch_failures)} project(s) with protected branches: "
        f"{', '.join(protected_branch_failures)}. "
        f"Manual intervention required — remove protected branches before retrying."
    )
    if contact_emails:
        msg += f" Contact: {', '.join(contact_emails)}"
    raise RuntimeError(msg)

# COMMAND ----------

# ── Step 3: Fill missing placeholders ────────────────────────────────────────

if not placeholders_enabled:
    logger.info("Placeholder fill DISABLED — skipping")
    need_to_create = 0
    created_count = 0
    create_failed = 0
else:
    need_to_create = target_placeholders - len(placeholders)

    if need_to_create <= 0:
        logger.info("No placeholders to create (%d exist, target %d)",
                    len(placeholders), target_placeholders)
        created_count = 0
        create_failed = 0
    else:
        # Find existing indices to avoid collisions
        existing_indices = set()
        for p in placeholders:
            if p["name"].startswith("fleet-placeholder-"):
                try:
                    existing_indices.add(int(p["name"].split("-")[-1]))
                except ValueError:
                    pass

        # Generate names for missing slots
        names_to_create = []
        idx = 0
        for _ in range(need_to_create):
            while idx in existing_indices:
                idx += 1
            names_to_create.append(f"fleet-placeholder-{idx:04d}")
            existing_indices.add(idx)
            idx += 1

        logger.info("Creating %d placeholders (quota=%d, real=%d, existing_ph=%d)",
                    len(names_to_create), quota, len(known_real_names), len(placeholders))

        def _create_one(name, max_retries=5):
            """Create a single placeholder with retry on transient errors."""
            for attempt in range(1, max_retries + 1):
                try:
                    create_project(name, display_name=name, custom_tags=[
                        {"key": "owner", "value": OWNER_PLACEHOLDER},
                        {"key": "managed_by", "value": "autoscaler"},
                    ])
                    return True
                except Exception as exc:
                    err = str(exc)
                    if "already exists" in err:
                        logger.info("%s already exists, treating as success", name)
                        return True
                    if attempt == max_retries:
                        logger.error("%s failed after %d attempts: %s", name, max_retries, err[:200])
                        raise
                    if "429" in err or "RATE_LIMIT" in err or "500" in err or "503" in err or "Timed out" in err:
                        backoff = min(30, 2 ** attempt)
                        logger.warning("%s attempt %d failed (%s), retrying in %ds",
                                       name, attempt, err[:120], backoff)
                        time.sleep(backoff)
                    else:
                        raise
            return False

        created_count = 0
        create_failed = 0
        t0 = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_name = {
                executor.submit(_create_one, name): name
                for name in names_to_create
            }
            for i, future in enumerate(concurrent.futures.as_completed(future_to_name), 1):
                name = future_to_name[future]
                try:
                    future.result()
                    created_count += 1
                except Exception as exc:
                    create_failed += 1
                    logger.error("Failed: %s — %s", name, str(exc)[:200])
                if i % 100 == 0 or i == len(names_to_create):
                    elapsed = time.time() - t0
                    logger.info("Fill progress: %d/%d (created=%d, failed=%d, %.1fs)",
                                i, len(names_to_create), created_count, create_failed, elapsed)

        elapsed = time.time() - t0
        logger.info("Fill done: created=%d, failed=%d in %.1fs", created_count, create_failed, elapsed)

        if create_failed:
            raise RuntimeError(f"Fill: {create_failed}/{len(names_to_create)} projects failed to create")

# COMMAND ----------

# ── Step 4: Suspend newly created placeholder endpoints ──────────────────────
# The platform creates endpoints in ACTIVE state. We disable them immediately
# after creation so compute scales to zero. Already-existing placeholders were
# suspended on a previous run, so we only touch the new ones.

if placeholders_enabled and created_count > 0:
    logger.info("Suspending endpoints for %d newly created placeholders", created_count)

    suspended_count = 0
    suspend_skipped = 0
    t0 = time.time()

    def _suspend_one(name):
        try:
            suspend_endpoint(name)
            return True
        except Exception as exc:
            err = str(exc)
            if "404" in err or "NOT_FOUND" in err:
                logger.warning("suspend: %s endpoint not found, skipping", name)
                return False
            raise

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_name = {
            executor.submit(_suspend_one, name): name
            for name in names_to_create
        }
        for i, future in enumerate(concurrent.futures.as_completed(future_to_name), 1):
            name = future_to_name[future]
            try:
                if future.result():
                    suspended_count += 1
                else:
                    suspend_skipped += 1
            except Exception as exc:
                suspend_skipped += 1
                logger.error("suspend: %s failed — %s", name, str(exc)[:200])
            if i % 200 == 0 or i == len(names_to_create):
                elapsed = time.time() - t0
                logger.info("Suspend progress: %d/%d (suspended=%d, skipped=%d, %.1fs)",
                            i, len(names_to_create), suspended_count, suspend_skipped, elapsed)

    elapsed = time.time() - t0
    logger.info("Suspend done: suspended=%d, skipped=%d in %.1fs",
                suspended_count, suspend_skipped, elapsed)
else:
    suspended_count = 0
    suspend_skipped = 0

# COMMAND ----------

# ── Summary ──────────────────────────────────────────────────────────────────

summary = {
    "quota": quota,
    "real_count": len(known_real_names),
    "target_placeholders": target_placeholders,
    "existing_placeholders": len(placeholders),
    "orphans_found": len(orphans),
    "deleted": deleted,
    "created": created_count,
    "create_failed": create_failed,
    "suspended": suspended_count,
    "suspend_skipped": suspend_skipped,
}

logger.info("=== SUMMARY ===")
for k, v in summary.items():
    logger.info("  %s: %s", k, v)

dbutils.notebook.exit(str(summary))
