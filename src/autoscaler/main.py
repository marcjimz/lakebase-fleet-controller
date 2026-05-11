"""Lakebase Fleet Autoscaler — manages placeholder instances and orphan cleanup.

Single-file script executed as a Databricks spark_python_task.
All logic is inlined because the serverless runtime's exec() context
does not support relative imports or __file__-based path discovery.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import sys
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
    state: str = ""
    capacity: str = ""
    effective_stopped: bool = False

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

    # ── Endpoint management (scale-to-zero) ──────────────────────────────

    def list_branches(self, project_id: str) -> list[dict]:
        """List all branches for a project."""
        resp = self._ws.api_client.do(
            "GET", f"{_PROJECTS_API}/{project_id}/branches"
        )
        return resp.get("branches", [])

    def list_endpoints(self, project_id: str, branch: str) -> list[dict]:
        """List all endpoints for a project branch."""
        resp = self._ws.api_client.do(
            "GET", f"{_PROJECTS_API}/{project_id}/branches/{branch}/endpoints"
        )
        return resp.get("endpoints", [])

    def enable_suspension(self, project_id: str, branch: str, endpoint_id: str) -> dict:
        """Enable scale-to-zero on a specific endpoint (suspend after 60s idle)."""
        return self._ws.api_client.do(
            "PATCH",
            f"{_PROJECTS_API}/{project_id}/branches/{branch}/endpoints/{endpoint_id}",
            body={
                "endpoint": {
                    "spec": {
                        "no_suspension": False,
                        "suspend_timeout_duration": "60s",
                    }
                },
                "update_mask": "spec.no_suspension,spec.suspend_timeout_duration",
            },
        )

    def enable_scale_to_zero_for_project(self, project_id: str, max_retries: int = 6) -> None:
        """Discover branches/endpoints and enable suspension on each.

        Retries endpoint discovery because new projects may not have
        endpoints available immediately after creation.
        """
        for attempt in range(1, max_retries + 1):
            branches = self.list_branches(project_id)
            if branches:
                break
            if attempt == max_retries:
                logger.warning("scale-to-zero: %s has no branches after %d attempts, skipping", project_id, max_retries)
                return
            time.sleep(5)

        for branch_obj in branches:
            branch_name = branch_obj.get("branch_id") or branch_obj.get("name", "").split("/")[-1]
            if not branch_name:
                continue

            for attempt in range(1, max_retries + 1):
                endpoints = self.list_endpoints(project_id, branch_name)
                if endpoints:
                    break
                if attempt == max_retries:
                    logger.warning("scale-to-zero: %s/%s has no endpoints after %d attempts, skipping",
                                   project_id, branch_name, max_retries)
                    break
                time.sleep(5)
            else:
                continue

            for ep in endpoints:
                ep_id = ep.get("endpoint_id") or ep.get("name", "").split("/")[-1]
                if not ep_id:
                    continue
                logger.info("scale-to-zero: enabling suspension on %s/%s/%s", project_id, branch_name, ep_id)
                self.enable_suspension(project_id, branch_name, ep_id)


# ── Safety guards ────────────────────────────────────────────────────────────

def _assert_not_dab_owned(inst: DatabaseInstance, action: str) -> None:
    if inst.get_tag("owner") == OWNER_DAB:
        raise RuntimeError(
            f"REFUSING to {action} DAB-managed instance: {inst.name} (uid={inst.uid})"
        )


# ── Reconciliation logic ────────────────────────────────────────────────────

def cleanup_orphans(client: LakebaseClient, known_real_names: set[str]) -> int:
    deleted = 0
    for inst in client.list_all_projects():
        owner = inst.get_tag("owner")
        if owner == OWNER_DAB and inst.name in known_real_names:
            continue
        if owner == OWNER_PLACEHOLDER:
            continue
        logger.warning("Deleting orphan: %s (owner=%s)", inst.name, owner)
        _assert_not_dab_owned(inst, "delete orphan")
        try:
            client.delete_project(inst.name)
            deleted += 1
        except Exception as exc:
            err = str(exc)
            if "protected branches" in err or "FAILED_PRECONDITION" in err:
                logger.warning("Skipping %s: has protected branches (manual cleanup needed)", inst.name)
            else:
                raise
    if deleted:
        logger.info("Orphan cleanup: deleted %d", deleted)
    return deleted


def shrink_placeholders(client: LakebaseClient, target: int) -> int:
    placeholders = [i for i in client.list_all_projects() if i.get_tag("owner") == OWNER_PLACEHOLDER]
    current = len(placeholders)
    if current <= target:
        logger.info("Shrink: no-op (%d <= %d)", current, target)
        return 0
    placeholders.sort(key=lambda i: i.creation_time)
    to_delete = placeholders[:current - target]
    for inst in to_delete:
        _assert_not_dab_owned(inst, "shrink")
        logger.info("Shrink: deleting %s", inst.name)
        client.delete_project(inst.name)
    logger.info("Shrunk: deleted %d placeholders", len(to_delete))
    return len(to_delete)


def fill_placeholders(client: LakebaseClient, target: int) -> int:
    placeholders = [i for i in client.list_all_projects() if i.get_tag("owner") == OWNER_PLACEHOLDER]
    current = len(placeholders)
    if current >= target:
        logger.info("Fill: no-op (%d >= %d)", current, target)
        return 0

    existing_indices: set[int] = set()
    for inst in placeholders:
        if inst.name.startswith("fleet-placeholder-"):
            try:
                existing_indices.add(int(inst.name.split("-")[-1]))
            except ValueError:
                pass

    created = 0
    idx = 0
    while current + created < target:
        while idx in existing_indices:
            idx += 1
        name = f"fleet-placeholder-{idx:04d}"
        logger.info("Fill: creating %s", name)
        client.create_project(name, display_name=name, custom_tags=[
            {"key": "owner", "value": OWNER_PLACEHOLDER},
            {"key": "managed_by", "value": "autoscaler"},
        ])
        existing_indices.add(idx)
        created += 1
        idx += 1

    logger.info("Filled: created %d placeholders", created)
    return created


def reconcile(client: LakebaseClient, known_real_names: set[str], quota: int) -> dict:
    target_real = len(known_real_names)
    target_placeholders = quota - target_real
    if target_placeholders < 0:
        raise ValueError(
            f"Real ({target_real}) exceeds quota ({quota})"
        )
    orphans = cleanup_orphans(client, known_real_names)
    shrunk = shrink_placeholders(client, target_placeholders)
    filled = fill_placeholders(client, target_placeholders)
    return {
        "target_real": target_real,
        "target_placeholders": target_placeholders,
        "quota": quota,
        "orphans_deleted": orphans,
        "placeholders_shrunk": shrunk,
        "placeholders_filled": filled,
    }


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
    """Create a single placeholder and enable scale-to-zero. Returns True on success."""
    for attempt in range(1, max_retries + 1):
        try:
            client.create_project(name, display_name=name, custom_tags=[
                {"key": "owner", "value": OWNER_PLACEHOLDER},
                {"key": "managed_by", "value": "autoscaler"},
            ])
            client.enable_scale_to_zero_for_project(name)
            return True
        except Exception as exc:
            err = str(exc)
            if attempt == max_retries:
                logger.error("batch_create: %s failed after %d attempts: %s", name, max_retries, err[:200])
                raise
            if "429" in err or "RATE_LIMIT" in err or "500" in err or "503" in err:
                backoff = min(30, 2 ** attempt)
                logger.warning("batch_create: %s attempt %d failed (%s), retrying in %ds",
                               name, attempt, err[:120], backoff)
                time.sleep(backoff)
            else:
                raise
    return False


def batch_create(client: LakebaseClient, create_names_raw: str) -> int:
    """Create placeholders concurrently using ThreadPoolExecutor.

    Receives a pipe-separated list of names. Each thread creates one project
    and then enables scale-to-zero on its endpoints.
    """
    if create_names_raw == "__NONE__":
        logger.info("batch_create: nothing to create (sentinel __NONE__)")
        return 0

    names = [n.strip() for n in create_names_raw.split("|") if n.strip()]
    if not names:
        logger.info("batch_create: empty name list, nothing to do")
        return 0

    logger.info("batch_create: creating %d placeholders with 50 threads", len(names))
    created = 0
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_name = {
            executor.submit(_create_one_with_retry, client, name): name
            for name in names
        }
        for i, future in enumerate(concurrent.futures.as_completed(future_to_name), 1):
            name = future_to_name[future]
            try:
                future.result()
                created += 1
            except Exception as exc:
                failed += 1
                logger.error("batch_create: %s failed: %s", name, str(exc)[:200])
            if i % 50 == 0 or i == len(names):
                logger.info("batch_create: progress %d/%d (created=%d, failed=%d)",
                            i, len(names), created, failed)

    logger.info("batch_create: done — created=%d, failed=%d", created, failed)
    if failed:
        raise RuntimeError(f"batch_create: {failed}/{len(names)} projects failed to create")
    return created


def enable_scale_to_zero(client: LakebaseClient) -> int:
    """One-time migration: enable scale-to-zero on all existing placeholders."""
    all_projects = client.list_all_projects()
    placeholders = [p for p in all_projects if p.get_tag("owner") == OWNER_PLACEHOLDER]

    if not placeholders:
        logger.info("enable_scale_to_zero: no placeholders found")
        return 0

    logger.info("enable_scale_to_zero: updating %d placeholders with 50 threads", len(placeholders))
    updated = 0
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_name = {
            executor.submit(client.enable_scale_to_zero_for_project, p.name): p.name
            for p in placeholders
        }
        for i, future in enumerate(concurrent.futures.as_completed(future_to_name), 1):
            name = future_to_name[future]
            try:
                future.result()
                updated += 1
            except Exception as exc:
                failed += 1
                logger.error("enable_scale_to_zero: %s failed: %s", name, str(exc)[:200])
            if i % 50 == 0 or i == len(placeholders):
                logger.info("enable_scale_to_zero: progress %d/%d (updated=%d, failed=%d)",
                            i, len(placeholders), updated, failed)

    logger.info("enable_scale_to_zero: done — updated=%d, failed=%d", updated, failed)
    return updated


# ── CLI entry point ──────────────────────────────────────────────────────────

MODES = ["shrink", "fill", "reconcile", "cleanup_orphans", "cleanup_batch",
         "batch_create", "enable_scale_to_zero"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lakebase Fleet Autoscaler")
    parser.add_argument("--mode", choices=MODES, default="reconcile")
    parser.add_argument("--real-names", default="", help="Pipe-separated instance names (a|b|c)")
    parser.add_argument("--quota", type=int, default=1000)
    # cleanup_batch args
    parser.add_argument("--delete-names", default="__NONE__", help="Pipe-separated names to delete")
    # batch_create args
    parser.add_argument("--create-names", default="__NONE__", help="Pipe-separated names to create")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logger.info("Mode=%s", args.mode)

    client = LakebaseClient()

    if args.mode == "cleanup_batch":
        cleanup_batch(client, args.delete_names)
    elif args.mode == "batch_create":
        batch_create(client, args.create_names)
    elif args.mode == "enable_scale_to_zero":
        enable_scale_to_zero(client)
    else:
        # Legacy modes that require real-names / quota
        known_real_names = {n.strip() for n in args.real_names.split("|") if n.strip()}
        quota = args.quota
        target_placeholders = quota - len(known_real_names)

        if target_placeholders < 0:
            logger.error("Real (%d) exceeds quota (%d)", len(known_real_names), quota)
            sys.exit(1)

        logger.info("quota=%d | real=%d | target_ph=%d",
                    quota, len(known_real_names), target_placeholders)
        logger.info("Real names: %s", known_real_names)

        if args.mode == "cleanup_orphans":
            cleanup_orphans(client, known_real_names)
        elif args.mode == "shrink":
            cleanup_orphans(client, known_real_names)
            shrink_placeholders(client, target_placeholders)
        elif args.mode == "fill":
            cleanup_orphans(client, known_real_names)
            fill_placeholders(client, target_placeholders)
        elif args.mode == "reconcile":
            result = reconcile(client, known_real_names, quota)
            logger.info("Result: %s", json.dumps(result, indent=2))

    logger.info("Done.")


if __name__ == "__main__":
    main()
