#!/usr/bin/env bash
# Pre-deploy state reconciliation — enforces the workspace invariant BEFORE
# bundle deploy so that deploy never fails due to capacity.
#
# Invariant: the ONLY instances allowed in the workspace are:
#   1. Real instances declared in the DAB bundle (owner=dab, name in REAL_NAMES)
#   2. Autoscaler placeholders (owner=autoscaler-placeholder)
#   Everything else is an orphan and gets deleted.
#
# After orphan cleanup, enough placeholders are deleted to guarantee free
# slots for any real instances that bundle deploy needs to create.
#
# Usage: bash scripts/pre_deploy_shrink.sh <real_names> <real_count>
#   real_names:  pipe-separated bundle instance names (e.g. "fleet-project-1|fleet-project-2")
#   real_count:  number of real instances (slots to guarantee)
#
# Requires: databricks CLI authenticated (DATABRICKS_HOST + credentials)
set -euo pipefail

REAL_NAMES="${1:?Usage: pre_deploy_shrink.sh <real_names> <real_count>}"
REAL_COUNT="${2:?Usage: pre_deploy_shrink.sh <real_names> <real_count>}"
PROJECTS_API="/api/2.0/postgres/projects"
OWNER_DAB="dab"
OWNER_PLACEHOLDER="autoscaler-placeholder"

echo "=== Pre-deploy state reconciliation ==="
echo "Real names: $REAL_NAMES"
echo "Real count: $REAL_COUNT"

# Build a jq-friendly array of allowed real names
REAL_NAMES_JSON=$(echo "$REAL_NAMES" | tr '|' '\n' | jq -R . | jq -s .)

# ── Phase 1: List all instances ──────────────────────────────────────────────

ALL_INSTANCES="[]"
PAGE_TOKEN=""

while true; do
  URL="${PROJECTS_API}"
  if [[ -n "$PAGE_TOKEN" ]]; then
    URL="${URL}?page_token=${PAGE_TOKEN}"
  fi

  RESP=$(databricks api get "$URL" 2>/dev/null || echo '{}')

  # Extract project_id, owner tag, and creation_time from each project
  PAGE_INSTANCES=$(echo "$RESP" | jq -c "[
    .projects // [] | .[] | {
      name: (.status.project_id // (.name | ltrimstr(\"projects/\"))),
      creation_time: .create_time,
      owner: (
        [(.status.custom_tags // [])[] | select(.key == \"owner\") | .value] | first // \"\"
      )
    }
  ]")

  ALL_INSTANCES=$(echo "$ALL_INSTANCES $PAGE_INSTANCES" | jq -s 'add')

  PAGE_TOKEN=$(echo "$RESP" | jq -r '.next_page_token // empty')
  if [[ -z "$PAGE_TOKEN" ]]; then
    break
  fi
done

TOTAL=$(echo "$ALL_INSTANCES" | jq 'length')
echo "Found $TOTAL total instance(s) in workspace"

if [[ "$TOTAL" -eq 0 ]]; then
  echo "Workspace is empty — nothing to reconcile"
  exit 0
fi

# ── Phase 2: Classify instances ──────────────────────────────────────────────

# Real: owner=dab AND name in the bundle's real_names list
REAL=$(echo "$ALL_INSTANCES" | jq -c --argjson allowed "$REAL_NAMES_JSON" \
  '[.[] | select(.owner == "dab" and (.name as $n | $allowed | index($n)))]')

# Placeholders: owner=autoscaler-placeholder
PLACEHOLDERS=$(echo "$ALL_INSTANCES" | jq -c \
  '[.[] | select(.owner == "autoscaler-placeholder")]')

# Orphans: everything else (not a known real instance, not a placeholder)
ORPHANS=$(echo "$ALL_INSTANCES" | jq -c --argjson allowed "$REAL_NAMES_JSON" \
  '[.[] | select(
    (.owner == "dab" and (.name as $n | $allowed | index($n))) | not
  ) | select(.owner != "autoscaler-placeholder")]')

REAL_FOUND=$(echo "$REAL" | jq 'length')
PLACEHOLDER_COUNT=$(echo "$PLACEHOLDERS" | jq 'length')
ORPHAN_COUNT=$(echo "$ORPHANS" | jq 'length')

echo "Classification: real=$REAL_FOUND, placeholders=$PLACEHOLDER_COUNT, orphans=$ORPHAN_COUNT"

# ── Phase 3: Delete orphans ──────────────────────────────────────────────────

DELETED=0

if [[ "$ORPHAN_COUNT" -gt 0 ]]; then
  echo "--- Deleting $ORPHAN_COUNT orphan(s) ---"
  ORPHAN_NAMES=$(echo "$ORPHANS" | jq -r '.[].name')
  while IFS= read -r NAME; do
    echo "  Deleting orphan: $NAME"
    if databricks api delete "${PROJECTS_API}/${NAME}" 2>/dev/null; then
      DELETED=$((DELETED + 1))
    else
      echo "    Warning: failed to delete $NAME (may already be gone)"
    fi
  done <<< "$ORPHAN_NAMES"
fi

# ── Phase 4: Shrink placeholders to free slots for real instances ────────────
# Only need free slots for real instances that DON'T already exist.
# Existing real instances already occupy a slot and don't need new ones.
# Free slots needed = (new real instances) - (orphans we just deleted).

NEW_REAL_NEEDED=$((REAL_COUNT - REAL_FOUND))
SLOTS_FREED=$DELETED
SLOTS_STILL_NEEDED=$((NEW_REAL_NEEDED - SLOTS_FREED))

echo "New real instances needed: $NEW_REAL_NEEDED (total=$REAL_COUNT, existing=$REAL_FOUND)"

if [[ "$SLOTS_STILL_NEEDED" -gt 0 && "$PLACEHOLDER_COUNT" -gt 0 ]]; then
  # Cap at available placeholders
  TO_SHRINK=$SLOTS_STILL_NEEDED
  if [[ "$TO_SHRINK" -gt "$PLACEHOLDER_COUNT" ]]; then
    TO_SHRINK=$PLACEHOLDER_COUNT
  fi

  echo "--- Shrinking $TO_SHRINK placeholder(s) to free slots ---"
  SHRINK_NAMES=$(echo "$PLACEHOLDERS" | jq -r "sort_by(.creation_time) | .[:$TO_SHRINK] | .[].name")
  while IFS= read -r NAME; do
    echo "  Deleting placeholder: $NAME"
    if databricks api delete "${PROJECTS_API}/${NAME}" 2>/dev/null; then
      DELETED=$((DELETED + 1))
    else
      echo "    Warning: failed to delete $NAME (may already be gone)"
    fi
  done <<< "$SHRINK_NAMES"
fi

echo "=== Pre-deploy reconciliation complete: deleted $DELETED instance(s) ==="
