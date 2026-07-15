#!/bin/bash
# Resolve the 4 new threads from Copilot re-review

THREADS=(
  "PRRT_kwDOS9Wxuc6REk0V"
  "PRRT_kwDOS9Wxuc6REk05"
  "PRRT_kwDOS9Wxuc6REk1Q"
  "PRRT_kwDOS9Wxuc6REk1w"
)

for THREAD_ID in "${THREADS[@]}"; do
  echo "Resolving thread: $THREAD_ID"
  gh api graphql -f query="mutation { resolveReviewThread(input: {threadId: \"$THREAD_ID\"}) { thread { id isResolved } } }"
done

echo ""
echo "Done. Verifying unresolved count with pagination..."
# Use --paginate to get ALL threads, not just first 100
gh api graphql --paginate -f query='query($cursor: String) { repository(owner: "intent-hq", name: "intentd") { pullRequest(number: 183) { reviewThreads(first: 100, after: $cursor) { pageInfo { hasNextPage endCursor } nodes { isResolved } } } } }' --jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false)' | wc -l
