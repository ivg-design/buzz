-- Backfill the conversational thread index for job events written before
-- protected job inserts persisted thread_metadata. The protocol's root e-tag
-- identifies the original kind:43001 request. Lifecycle predecessor tags are
-- intentionally not used as parents: the user-facing task thread is flat.

WITH job_followups AS (
    SELECT
        followup.community_id,
        followup.channel_id,
        followup.created_at,
        followup.id,
        decode(root_tag.value->>1, 'hex') AS root_id
    FROM events followup
    CROSS JOIN LATERAL (
        SELECT value
        FROM jsonb_array_elements(followup.tags) AS tag(value)
        WHERE value->>0 = 'e'
          AND value->>3 = 'root'
          AND value->>1 ~ '^[0-9a-fA-F]{64}$'
        LIMIT 1
    ) root_tag
    WHERE followup.kind BETWEEN 43002 AND 43006
      AND followup.channel_id IS NOT NULL
      AND followup.deleted_at IS NULL
), valid_followups AS (
    SELECT followup.*, root.created_at AS root_created_at
    FROM job_followups followup
    JOIN events root
      ON root.community_id = followup.community_id
     AND root.id = followup.root_id
     AND root.channel_id = followup.channel_id
     AND root.kind = 43001
     AND root.deleted_at IS NULL
)
INSERT INTO thread_metadata (
    community_id,
    event_created_at,
    event_id,
    channel_id,
    parent_event_id,
    parent_event_created_at,
    root_event_id,
    root_event_created_at,
    depth,
    broadcast
)
SELECT
    community_id,
    root_created_at,
    root_id,
    channel_id,
    NULL,
    NULL,
    NULL,
    NULL,
    0,
    FALSE
FROM valid_followups
ON CONFLICT DO NOTHING;

WITH job_followups AS (
    SELECT
        followup.community_id,
        followup.channel_id,
        followup.created_at,
        followup.id,
        decode(root_tag.value->>1, 'hex') AS root_id
    FROM events followup
    CROSS JOIN LATERAL (
        SELECT value
        FROM jsonb_array_elements(followup.tags) AS tag(value)
        WHERE value->>0 = 'e'
          AND value->>3 = 'root'
          AND value->>1 ~ '^[0-9a-fA-F]{64}$'
        LIMIT 1
    ) root_tag
    WHERE followup.kind BETWEEN 43002 AND 43006
      AND followup.channel_id IS NOT NULL
      AND followup.deleted_at IS NULL
), valid_followups AS (
    SELECT followup.*, root.created_at AS root_created_at
    FROM job_followups followup
    JOIN events root
      ON root.community_id = followup.community_id
     AND root.id = followup.root_id
     AND root.channel_id = followup.channel_id
     AND root.kind = 43001
     AND root.deleted_at IS NULL
)
INSERT INTO thread_metadata (
    community_id,
    event_created_at,
    event_id,
    channel_id,
    parent_event_id,
    parent_event_created_at,
    root_event_id,
    root_event_created_at,
    depth,
    broadcast
)
SELECT
    community_id,
    created_at,
    id,
    channel_id,
    root_id,
    root_created_at,
    root_id,
    root_created_at,
    1,
    FALSE
FROM valid_followups
ON CONFLICT DO NOTHING;

-- Recompute affected root summaries from the complete metadata set so the
-- repair is idempotent and preserves already-indexed human discussion.
WITH affected_roots AS (
    SELECT DISTINCT metadata.community_id, metadata.root_event_id
    FROM thread_metadata metadata
    JOIN events followup
      ON followup.community_id = metadata.community_id
     AND followup.id = metadata.event_id
     AND followup.kind BETWEEN 43002 AND 43006
    WHERE metadata.root_event_id IS NOT NULL
), summaries AS (
    SELECT
        roots.community_id,
        roots.root_event_id,
        COUNT(*) FILTER (
            WHERE metadata.parent_event_id = roots.root_event_id
        )::INT AS reply_count,
        COUNT(metadata.event_id)::INT AS descendant_count,
        MAX(metadata.event_created_at) FILTER (
            WHERE metadata.parent_event_id = roots.root_event_id
        ) AS last_reply_at
    FROM affected_roots roots
    LEFT JOIN thread_metadata metadata
      ON metadata.community_id = roots.community_id
     AND metadata.root_event_id = roots.root_event_id
    GROUP BY roots.community_id, roots.root_event_id
)
UPDATE thread_metadata root
SET reply_count = summaries.reply_count,
    descendant_count = summaries.descendant_count,
    last_reply_at = summaries.last_reply_at
FROM summaries
WHERE root.community_id = summaries.community_id
  AND root.event_id = summaries.root_event_id;
