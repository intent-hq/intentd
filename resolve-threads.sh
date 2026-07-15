#!/bin/bash
# Resolve all threads on PR #183

# Get all unresolved thread IDs
THREAD_IDS=$(gh api graphql -f query='query { repository(owner: "intent-hq", name: "intentd") { pullRequest(number: 183) { reviewThreads(first: 100) { nodes { id isResolved } } } } }' --jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false) | .id')

# Resolve each thread
for THREAD_ID in $THREAD_IDS; do
  echo "Resolving thread: $THREAD_ID"
  gh api graphql -f query="mutation { resolveReviewThread(input: {threadId: \"$THREAD_ID\"}) { thread { id isResolved } } }"
done

echo "Done. Verifying unresolved count..."
gh api graphql -f query='query { repository(owner: "intent-hq", name: "intentd") { pullRequest(number: 183) { reviewThreads(first: 100) { nodes { isResolved } } } } }' --jq '[.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false)] | length'
