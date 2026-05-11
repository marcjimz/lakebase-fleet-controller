"""Lakebase Fleet Autoscaler — manages placeholder instances and orphan cleanup.

Single-file script executed as a Databricks spark_python_task.
All logic is inlined because the serverless runtime's exec() context
does not support relative imports or __file__-based path discovery.
"""

from __future__ import annotations

import argparse
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
_API_BASE = "/api/2.0/database/instances"


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

    def list_all_instances(self) -> list[DatabaseInstance]:
        instances = []
        page_token = None
        while True:
            url = f"{_API_BASE}?include_custom_tags=true"
            if page_token:
                url = f"{url}&page_token={page_token}"
            resp = self._ws.api_client.do("GET", url)
            for item in resp.get("database_instances", []):
                # API returns tags under effective_custom_tags (merged view)
                tags = item.get("effective_custom_tags") or item.get("custom_tags") or []
                instances.append(DatabaseInstance(
                    name=item.get("name", ""),
                    uid=item.get("uid", ""),
                    custom_tags=tags,
                    creation_time=item.get("creation_time", ""),
                    state=item.get("state", ""),
                    capacity=item.get("capacity", ""),
                    effective_stopped=item.get("effective_stopped", False),
                ))
            page_token = resp.get("next_page_token")
            if not page_token:
                break
        return instances

    def create_instance(self, config: dict) -> dict:
        logger.info("Creating instance: %s", config.get("name", "?"))
        return self._ws.api_client.do("POST", _API_BASE, body=config)

    def delete_instance(self, name: str) -> None:
        logger.info("Deleting instance: %s", name)
        self._ws.api_client.do("DELETE", f"{_API_BASE}/{name}")

    def get_instance(self, name: str) -> dict:
        return self._ws.api_client.do("GET", f"{_API_BASE}/{name}")

    def wait_for_state(self, name: str, target: str, timeout: int = 600, interval: int = 10) -> str:
        """Poll until instance reaches target state or timeout."""
        deadline = time.time() + timeout
        while True:
            resp = self.get_instance(name)
            state = resp.get("state", "")
            if state == target:
                logger.info("Instance %s reached state %s", name, target)
                return state
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Instance {name} did not reach {target} within {timeout}s (current: {state})"
                )
            logger.info("Waiting for %s: state=%s, target=%s", name, state, target)
            time.sleep(interval)

    def stop_instance(self, name: str) -> None:
        logger.info("Stopping instance: %s", name)
        self._ws.api_client.do("PATCH", f"{_API_BASE}/{name}", body={"stopped": True})


# ── Safety guards ────────────────────────────────────────────────────────────

def _assert_not_dab_owned(inst: DatabaseInstance, action: str) -> None:
    if inst.get_tag("owner") == OWNER_DAB:
        raise RuntimeError(
            f"REFUSING to {action} DAB-managed instance: {inst.name} (uid={inst.uid})"
        )


# ── Reconciliation logic ────────────────────────────────────────────────────

def cleanup_orphans(client: LakebaseClient, known_real_names: set[str]) -> int:
    deleted = 0
    for inst in client.list_all_instances():
        owner = inst.get_tag("owner")
        if owner == OWNER_DAB and inst.name in known_real_names:
            continue
        if owner == OWNER_PLACEHOLDER:
            continue
        logger.warning("Deleting orphan: %s (owner=%s)", inst.name, owner)
        _assert_not_dab_owned(inst, "delete orphan")
        client.delete_instance(inst.name)
        deleted += 1
    if deleted:
        logger.info("Orphan cleanup: deleted %d", deleted)
    return deleted


def shrink_placeholders(client: LakebaseClient, target: int) -> int:
    placeholders = [i for i in client.list_all_instances() if i.get_tag("owner") == OWNER_PLACEHOLDER]
    current = len(placeholders)
    if current <= target:
        logger.info("Shrink: no-op (%d <= %d)", current, target)
        return 0
    placeholders.sort(key=lambda i: i.creation_time)
    to_delete = placeholders[:current - target]
    for inst in to_delete:
        _assert_not_dab_owned(inst, "shrink")
        logger.info("Shrink: deleting %s", inst.name)
        client.delete_instance(inst.name)
    logger.info("Shrunk: deleted %d placeholders", len(to_delete))
    return len(to_delete)


def fill_placeholders(client: LakebaseClient, target: int) -> int:
    placeholders = [i for i in client.list_all_instances() if i.get_tag("owner") == OWNER_PLACEHOLDER]
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
        logger.info("Fill: creating %s then stopping", name)
        client.create_instance({
            "name": name,
            "capacity": "CU_1",
            "custom_tags": [
                {"key": "owner", "value": OWNER_PLACEHOLDER},
                {"key": "managed_by", "value": "autoscaler"},
            ],
        })
        # Wait for instance to become AVAILABLE before stopping
        client.wait_for_state(name, "AVAILABLE")
        client.stop_instance(name)
        existing_indices.add(idx)
        created += 1
        idx += 1

    logger.info("Filled: created %d placeholders", created)
    return created


def reconcile(client: LakebaseClient, known_real_names: set[str], quota: int, headroom: int) -> dict:
    target_real = len(known_real_names)
    target_placeholders = quota - target_real - headroom
    if target_placeholders < 0:
        raise ValueError(
            f"Real ({target_real}) + headroom ({headroom}) exceeds quota ({quota})"
        )
    orphans = cleanup_orphans(client, known_real_names)
    shrunk = shrink_placeholders(client, target_placeholders)
    filled = fill_placeholders(client, target_placeholders)
    return {
        "target_real": target_real,
        "target_placeholders": target_placeholders,
        "quota": quota,
        "headroom": headroom,
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
        # Safety: refuse to delete DAB-owned instances
        try:
            resp = client.get_instance(name)
        except Exception as exc:
            if "404" in str(exc) or "NOT_FOUND" in str(exc):
                logger.info("cleanup_batch: %s already gone (404), skipping", name)
                continue
            raise
        tags = resp.get("effective_custom_tags") or resp.get("custom_tags") or []
        owner = _get_tag(tags, "owner")
        if owner == OWNER_DAB:
            raise RuntimeError(f"REFUSING to delete DAB-managed instance: {name}")
        logger.info("cleanup_batch: deleting %s (owner=%s)", name, owner)
        client.delete_instance(name)
        deleted += 1

    logger.info("cleanup_batch: deleted %d instances", deleted)
    return deleted


def create_one(client: LakebaseClient, instance_name: str) -> None:
    """Create a single placeholder instance, wait for AVAILABLE, then stop it."""
    if instance_name == "__SKIP__":
        logger.info("create_one: nothing to create (sentinel __SKIP__)")
        return

    logger.info("create_one: creating %s", instance_name)
    client.create_instance({
        "name": instance_name,
        "capacity": "CU_1",
        "custom_tags": [
            {"key": "owner", "value": OWNER_PLACEHOLDER},
            {"key": "managed_by", "value": "autoscaler"},
        ],
    })
    client.wait_for_state(instance_name, "AVAILABLE")
    client.stop_instance(instance_name)
    logger.info("create_one: %s created and stopped", instance_name)


# ── CLI entry point ──────────────────────────────────────────────────────────

MODES = ["shrink", "fill", "reconcile", "cleanup_orphans", "cleanup_batch", "create_one"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lakebase Fleet Autoscaler")
    parser.add_argument("--mode", choices=MODES, default="reconcile")
    parser.add_argument("--real-names", default="", help="Pipe-separated instance names (a|b|c)")
    parser.add_argument("--quota", type=int, default=1000)
    parser.add_argument("--headroom", type=int, default=10)
    # cleanup_batch args
    parser.add_argument("--delete-names", default="__NONE__", help="Pipe-separated names to delete")
    # create_one args
    parser.add_argument("--instance-name", default="__SKIP__", help="Single instance name to create")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logger.info("Mode=%s", args.mode)

    client = LakebaseClient()

    if args.mode == "cleanup_batch":
        cleanup_batch(client, args.delete_names)
    elif args.mode == "create_one":
        create_one(client, args.instance_name)
    else:
        # Legacy modes that require real-names / quota / headroom
        known_real_names = {n.strip() for n in args.real_names.split("|") if n.strip()}
        quota = args.quota
        headroom = args.headroom
        target_placeholders = quota - len(known_real_names) - headroom

        if target_placeholders < 0:
            logger.error("Real (%d) + headroom (%d) exceeds quota (%d)", len(known_real_names), headroom, quota)
            sys.exit(1)

        logger.info("quota=%d | real=%d | headroom=%d | target_ph=%d",
                    quota, len(known_real_names), headroom, target_placeholders)
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
            result = reconcile(client, known_real_names, quota, headroom)
            logger.info("Result: %s", json.dumps(result, indent=2))

    logger.info("Done.")


if __name__ == "__main__":
    main()
