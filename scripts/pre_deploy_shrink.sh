#!/usr/bin/env bash
# Pre-deploy shrink — deletes placeholder instances to free slots for bundle deploy.
#
# When the workspace is at capacity, `databricks bundle deploy` cannot create
# new real instances. This script frees enough slots BEFORE deploy by deleting
# the oldest autoscaler-placeholder instances via the Lakebase API.
#
# Usage: bash scripts/pre_deploy_shrink.sh <slots_needed>
#   slots_needed: number of placeholder instances to delete (typically = real instance count)
#
# Requires: databricks CLI authenticated (DATABRICKS_HOST + credentials)
set -euo pipefail

SLOTS_NEEDED="${1:?Usage: pre_deploy_shrink.sh <slots_needed>}"
API_BASE="/api/2.0/database/instances"
OWNER_PLACEHOLDER="autoscaler-placeholder"

echo "Pre-deploy shrink: freeing $SLOTS_NEEDED slot(s) for bundle deploy"

# Collect all placeholder instances across pages
ALL_PLACEHOLDERS="[]"
PAGE_TOKEN=""

while true; do
  URL="${API_BASE}?include_custom_tags=true"
  if [[ -n "$PAGE_TOKEN" ]]; then
    URL="${URL}&page_token=${PAGE_TOKEN}"
  fi

  RESP=$(databricks api get "$URL" 2>/dev/null || echo '{}')

  # Extract placeholders from this page: instances with owner=autoscaler-placeholder
  PAGE_PLACEHOLDERS=$(echo "$RESP" | jq -c "[
    .database_instances // [] | .[] |
    select(
      (.effective_custom_tags // .custom_tags // [])[] |
      .key == \"owner\" and .value == \"$OWNER_PLACEHOLDER\"
    ) | { name, creation_time }
  ]")

  ALL_PLACEHOLDERS=$(echo "$ALL_PLACEHOLDERS $PAGE_PLACEHOLDERS" | jq -s 'add')

  PAGE_TOKEN=$(echo "$RESP" | jq -r '.next_page_token // empty')
  if [[ -z "$PAGE_TOKEN" ]]; then
    break
  fi
done

TOTAL=$(echo "$ALL_PLACEHOLDERS" | jq 'length')
echo "Found $TOTAL placeholder instance(s)"

if [[ "$TOTAL" -eq 0 ]]; then
  echo "No placeholders to delete — workspace may have room already"
  exit 0
fi

# Sort by creation_time (oldest first) and take only what we need
TO_DELETE=$(echo "$ALL_PLACEHOLDERS" | jq -r "sort_by(.creation_time) | .[:$SLOTS_NEEDED] | .[].name")

if [[ -z "$TO_DELETE" ]]; then
  echo "Nothing to delete"
  exit 0
fi

DELETED=0
while IFS= read -r NAME; do
  echo "Deleting placeholder: $NAME"
  if databricks api delete "${API_BASE}/${NAME}" 2>/dev/null; then
    DELETED=$((DELETED + 1))
  else
    echo "  Warning: failed to delete $NAME (may already be gone)"
  fi
done <<< "$TO_DELETE"

echo "Pre-deploy shrink complete: deleted $DELETED placeholder(s)"
