#!/bin/bash
threads=(
PRRT_kwDOS9Wxuc6QwftX
PRRT_kwDOS9Wxuc6QwfuF
PRRT_kwDOS9Wxuc6Qwfub
PRRT_kwDOS9Wxuc6Qwyly
PRRT_kwDOS9Wxuc6Qwymf
PRRT_kwDOS9Wxuc6QxGPa
PRRT_kwDOS9Wxuc6QxGQS
PRRT_kwDOS9Wxuc6QxGQo
PRRT_kwDOS9Wxuc6QxGRs
PRRT_kwDOS9Wxuc6QxGSF
PRRT_kwDOS9Wxuc6QxYKO
PRRT_kwDOS9Wxuc6QxYK5
PRRT_kwDOS9Wxuc6QxYLT
PRRT_kwDOS9Wxuc6QxrE-
PRRT_kwDOS9Wxuc6QxrFx
PRRT_kwDOS9Wxuc6Qx8Qq
PRRT_kwDOS9Wxuc6Qx8Re
PRRT_kwDOS9Wxuc6Qx8SS
PRRT_kwDOS9Wxuc6QyE7y
PRRT_kwDOS9Wxuc6QyE8Y
PRRT_kwDOS9Wxuc6QyE81
PRRT_kwDOS9Wxuc6QyS67
PRRT_kwDOS9Wxuc6QyS7d
PRRT_kwDOS9Wxuc6QyS7s
PRRT_kwDOS9Wxuc6QyorM
PRRT_kwDOS9Wxuc6Qyor5
PRRT_kwDOS9Wxuc6QyosU
PRRT_kwDOS9Wxuc6Qyosq
PRRT_kwDOS9Wxuc6QyotC
PRRT_kwDOS9Wxuc6QyyT2
)

for thread_id in "${threads[@]}"; do
  gh api graphql -f query='mutation($id: ID!) { resolveReviewThread(input: {threadId: $id}) { thread { id } } }' -f id="$thread_id" > /dev/null 2>&1
  echo "Resolved $thread_id"
done
