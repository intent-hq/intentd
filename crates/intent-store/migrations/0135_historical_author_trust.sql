-- These keys were previously arbitrary client metadata, not authenticated
-- historical authors. Establish a trusted-writer boundary without backfilling
-- people or changing any text, identity, timestamp or unrelated metadata.
UPDATE agent_message
SET metadata = json_remove(metadata, '$.humanAuthor')
WHERE CASE WHEN json_valid(metadata) THEN
    json_type(metadata, '$.humanAuthor') IS NOT NULL ELSE 0 END;

UPDATE agent_queue
SET payload = json_remove(payload, '$.messageMetadata.humanAuthor')
WHERE CASE WHEN json_valid(payload) THEN
    json_type(payload, '$.messageMetadata.humanAuthor') IS NOT NULL ELSE 0 END;

-- Legacy comment extras were similarly untyped. None of these fields was an
-- authenticated creation stamp before this migration.
UPDATE comment
SET extra_json = json_remove(extra_json, '$.authorPrincipalId', '$.authorIdentity',
    '$.sourceAuthorPrincipalId')
WHERE CASE WHEN json_valid(extra_json) THEN
    json_type(extra_json, '$.authorPrincipalId') IS NOT NULL OR
    json_type(extra_json, '$.authorIdentity') IS NOT NULL OR
    json_type(extra_json, '$.sourceAuthorPrincipalId') IS NOT NULL ELSE 0 END;
