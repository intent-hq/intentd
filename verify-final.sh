#!/bin/bash
# Final verification for PR #183

# 1. Resolve the 2 design-rationale threads (no code change needed)
echo "Resolving design-rationale threads PRRT_kwDOS9Wxuc6RFVD4 and PRRT_kwDOS9Wxuc6RFVEa..."
for THREAD_ID in "PRRT_kwDOS9Wxuc6RFVD4" "PRRT_kwDOS9Wxuc6RFVEa"; do
  gh api graphql -f query="mutation { resolveReviewThread(input: {threadId: \"$THREAD_ID\"}) { thread { id isResolved } } }"
done

echo ""
echo "2. Running paginated zero-check..."
# Get current timestamp
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "Timestamp: $TIMESTAMP"

# Paginated count
COUNT=$(gh api graphql --paginate -f query='query($cursor: String) { repository(owner: "intent-hq", name: "intentd") { pullRequest(number: 183) { reviewThreads(first: 100, after: $cursor) { pageInfo { hasNextPage endCursor } nodes { isResolved } } } } }' --jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false)' | wc -l)

echo "Unresolved count (paginated): $COUNT"
echo "Timestamp: $TIMESTAMP"
