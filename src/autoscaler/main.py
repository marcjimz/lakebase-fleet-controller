"""Lakebase Fleet Autoscaler — manages placeholder instances and orphan cleanup.

Single-file script executed as a Databricks spark_python_task.
All logic is inlined because the serverless runtime's exec() context
does not support relative imports or __file__-based path discovery.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("autoscaler")

# ── Constants ────────────────────────────────────────────────────────────────

OWNER_DAB = "dab"
OWNER_PLACEHOLDER = "autoscaler-placeholder"
_PROJECTS_API = "/api/2.0/postgres/projects"


# ── Data model ───────────────────────────────────────────────────────────────

def _get_tag(tags: list[dict], key: str) -> Optional[str]:
    for t in tags:
        if t.get("key") == key:
            return t.get("value")
    return None


@dataclass
class DatabaseInstance:
    name: str
    uid: str = ""
    custom_tags: list[dict] = field(default_factory=list)
    creation_time: str = ""

    def get_tag(self, key: str) -> Optional[str]:
        return _get_tag(self.custom_tags, key)


# ── API client ───────────────────────────────────────────────────────────────

class LakebaseClient:
    def __init__(self):
        from databricks.sdk import WorkspaceClient
        self._ws = WorkspaceClient()

    def list_all_projects(self) -> list[DatabaseInstance]:
        """List ALL Lakebase projects via /api/2.0/postgres/projects.

        This is the authoritative listing that sees every project in the
        workspace, unlike /api/2.0/database/instances which only returns
        a subset.
        """
        instances = []
        page_token = None
        while True:
            url = _PROJECTS_API
            if page_token:
                url = f"{url}?page_token={page_token}"
            resp = self._ws.api_client.do("GET", url)
            for item in resp.get("projects", []):
                status = item.get("status", {})
                tags = status.get("custom_tags") or []
                # name is "projects/{id}" — extract the project_id
                project_id = status.get("project_id") or item.get("name", "").removeprefix("projects/")
                instances.append(DatabaseInstance(
                    name=project_id,
                    uid=item.get("uid", ""),
                    custom_tags=tags,
                    creation_time=item.get("create_time", ""),
                ))
            page_token = resp.get("next_page_token")
            if not page_token:
                break
        return instances

    def create_project(self, project_id: str, display_name: str, custom_tags: list[dict] | None = None) -> dict:
        """Create a Lakebase project via the projects API.

        project_id is sent as a query parameter (AIP-style), not in the body.
        """
        logger.info("Creating project: %s", project_id)
        body: dict = {"spec": {"display_name": display_name}}
        if custom_tags:
            body["spec"]["custom_tags"] = custom_tags
        return self._ws.api_client.do(
            "POST", f"{_PROJECTS_API}?project_id={project_id}", body=body
        )

    def delete_project(self, project_id: str) -> None:
        logger.info("Deleting project: %s", project_id)
        self._ws.api_client.do("DELETE", f"{_PROJECTS_API}/{project_id}")

    def get_project(self, project_id: str) -> dict:
        return self._ws.api_client.do("GET", f"{_PROJECTS_API}/{project_id}")

    def get_project_tags(self, project_id: str) -> list[dict]:
        resp = self.get_project(project_id)
        return resp.get("status", {}).get("custom_tags") or []



# ── DAG task modes ───────────────────────────────────────────────────────────

def cleanup_batch(client: LakebaseClient, delete_names_raw: str) -> int:
    """Delete a pipe-separated list of instances. Skips __NONE__ sentinel and 404s."""
    if delete_names_raw == "__NONE__":
        logger.info("cleanup_batch: nothing to delete (sentinel __NONE__)")
        return 0

    names = [n.strip() for n in delete_names_raw.split("|") if n.strip()]
    deleted = 0
    for name in names:
        # Safety: refuse to delete DAB-owned projects
        try:
            tags = client.get_project_tags(name)
        except Exception as exc:
            if "404" in str(exc) or "NOT_FOUND" in str(exc):
                logger.info("cleanup_batch: %s already gone (404), skipping", name)
                continue
            raise
        owner = _get_tag(tags, "owner")
        if owner == OWNER_DAB:
            raise RuntimeError(f"REFUSING to delete DAB-managed project: {name}")
        logger.info("cleanup_batch: deleting %s (owner=%s)", name, owner)
        try:
            client.delete_project(name)
            deleted += 1
        except Exception as exc:
            err = str(exc)
            if "protected branches" in err or "FAILED_PRECONDITION" in err:
                logger.warning("cleanup_batch: %s has protected branches, skipping (manual cleanup needed)", name)
                continue
            if "404" in err or "NOT_FOUND" in err:
                logger.info("cleanup_batch: %s already gone, skipping", name)
                continue
            raise

    logger.info("cleanup_batch: deleted %d instances", deleted)
    return deleted


def _create_one_with_retry(client: LakebaseClient, name: str, max_retries: int = 5) -> bool:
    """Create a single placeholder project. Returns True on success."""
    for attempt in range(1, max_retries + 1):
        try:
            client.create_project(name, display_name=name, custom_tags=[
                {"key": "owner", "value": OWNER_PLACEHOLDER},
                {"key": "managed_by", "value": "autoscaler"},
            ])
            return True
        except Exception as exc:
            err = str(exc)
            if attempt == max_retries:
                logger.error("create: %s failed after %d attempts: %s", name, max_retries, err[:200])
                raise
            if "429" in err or "RATE_LIMIT" in err or "500" in err or "503" in err:
                backoff = min(30, 2 ** attempt)
                logger.warning("create: %s attempt %d failed (%s), retrying in %ds",
                               name, attempt, err[:120], backoff)
                time.sleep(backoff)
            else:
                raise
    return False


def batch_create(client: LakebaseClient, quota: int, slice_num: str, total_slices: int) -> int:
    """Create missing placeholders for a single slice.

    Self-contained: lists all projects, computes the full deterministic list
    of needed placeholder names, then takes its partition (index % total_slices == slice_num).
    Scale-to-zero is the platform default — no post-creation PATCH needed.
    """
    if slice_num == "__SKIP__":
        logger.info("batch_create: skipping (sentinel __SKIP__)")
        return 0

    my_slice = int(slice_num)

    all_projects = client.list_all_projects()
    placeholders = [p for p in all_projects if p.get_tag("owner") == OWNER_PLACEHOLDER]
    real_count = sum(1 for p in all_projects if p.get_tag("owner") == OWNER_DAB)
    target_placeholders = quota - real_count

    if target_placeholders < 0:
        raise ValueError(f"Real ({real_count}) exceeds quota ({quota})")

    need_to_create = target_placeholders - len(placeholders)
    if need_to_create <= 0:
        logger.info("batch_create[%d]: no-op (%d placeholders >= target %d)", my_slice, len(placeholders), target_placeholders)
        return 0

    # Find existing indices to avoid collisions
    existing_indices: set[int] = set()
    for p in placeholders:
        if p.name.startswith("fleet-placeholder-"):
            try:
                existing_indices.add(int(p.name.split("-")[-1]))
            except ValueError:
                pass

    # Generate the FULL deterministic list of names, then take our slice
    all_names: list[str] = []
    idx = 0
    for _ in range(need_to_create):
        while idx in existing_indices:
            idx += 1
        all_names.append(f"fleet-placeholder-{idx:04d}")
        existing_indices.add(idx)
        idx += 1

    my_names = [n for i, n in enumerate(all_names) if i % total_slices == my_slice]

    if not my_names:
        logger.info("batch_create[%d]: no names in this slice", my_slice)
        return 0

    logger.info("batch_create[%d]: creating %d/%d placeholders (quota=%d, real=%d, existing_ph=%d)",
                my_slice, len(my_names), len(all_names), quota, real_count, len(placeholders))

    # ── Phase 1: Create all projects ──────────────────────────────────────
    created_names: list[str] = []
    create_failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_name = {
            executor.submit(_create_one_with_retry, client, name): name
            for name in my_names
        }
        for i, future in enumerate(concurrent.futures.as_completed(future_to_name), 1):
            name = future_to_name[future]
            try:
                future.result()
                created_names.append(name)
            except Exception as exc:
                create_failed += 1
                logger.error("batch_create[%d]: %s failed: %s", my_slice, name, str(exc)[:200])
            if i % 50 == 0 or i == len(my_names):
                logger.info("batch_create[%d]: phase1 progress %d/%d (created=%d, failed=%d)",
                            my_slice, i, len(my_names), len(created_names), create_failed)

    logger.info("batch_create[%d]: done — created=%d, failed=%d", my_slice, len(created_names), create_failed)
    if create_failed:
        raise RuntimeError(f"batch_create[{my_slice}]: {create_failed}/{len(my_names)} projects failed to create")
    return len(created_names)


# ── CLI entry point ──────────────────────────────────────────────────────────

MODES = ["cleanup_batch", "batch_create"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lakebase Fleet Autoscaler")
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--quota", type=int, default=1000)
    parser.add_argument("--delete-names", default="__NONE__", help="Pipe-separated names to delete")
    parser.add_argument("--slice", default="__SKIP__", help="Slice number for for_each_task")
    parser.add_argument("--total-slices", type=int, default=10, help="Total number of slices")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logger.info("Mode=%s", args.mode)

    client = LakebaseClient()

    if args.mode == "cleanup_batch":
        cleanup_batch(client, args.delete_names)
    elif args.mode == "batch_create":
        batch_create(client, args.quota, args.slice, args.total_slices)

    logger.info("Done.")


if __name__ == "__main__":
    main()
