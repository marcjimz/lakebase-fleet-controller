#!/usr/bin/env bash
# CI helper — assert every database_instances resource has prevent_destroy: true.
set -euo pipefail

TARGET="${1:-dev}"

echo "Verifying lifecycle.prevent_destroy on all database_instances for target: $TARGET"

BUNDLE_JSON=$(databricks bundle validate -t "$TARGET" -o json 2>/dev/null)

UNPROTECTED=$(echo "$BUNDLE_JSON" | jq -r '
  .resources.database_instances // {} | to_entries[] |
  select(.value.lifecycle.prevent_destroy != true) |
  .key
')

if [[ -n "$UNPROTECTED" ]]; then
  echo "ERROR: The following database_instances are missing lifecycle.prevent_destroy: true" >&2
  echo "$UNPROTECTED" >&2
  echo "" >&2
  echo "To retire a project, use a two-PR process:" >&2
  echo "  1. First PR: set prevent_destroy to false" >&2
  echo "  2. Second PR: remove the resource from databricks.yml" >&2
  exit 1
fi

echo "All database_instances have prevent_destroy enabled."
