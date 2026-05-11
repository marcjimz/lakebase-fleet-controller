#!/usr/bin/env bash
# CI helper — verify postgres_projects resources are properly configured.
set -euo pipefail

TARGET="${1:-dev}"

echo "Verifying postgres_projects for target: $TARGET"

BUNDLE_JSON=$(databricks bundle validate -t "$TARGET" -o json 2>/dev/null)

# Check that all projects have the owner=dab tag
UNTAGGED=$(echo "$BUNDLE_JSON" | jq -r '
  .resources.postgres_projects // {} | to_entries[] |
  select(
    (.value.custom_tags // []) | map(select(.key == "owner" and .value == "dab")) | length == 0
  ) |
  .key
')

if [[ -n "$UNTAGGED" ]]; then
  echo "ERROR: The following postgres_projects are missing owner=dab tag:" >&2
  echo "$UNTAGGED" >&2
  exit 1
fi

echo "All postgres_projects are properly tagged."
