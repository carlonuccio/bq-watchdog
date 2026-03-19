-- tests/fixtures/missing_partition.sql
-- Should trigger: missing_partition_filter (warn)

SELECT
    user_id,
    event_type,
    event_timestamp
FROM `project.dataset.events`
