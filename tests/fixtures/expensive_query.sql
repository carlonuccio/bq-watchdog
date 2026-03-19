-- tests/fixtures/expensive_query.sql
-- Should trigger: select_star (warn) + missing_partition_filter (warn)
-- Represents the classic "someone forgot filters" mistake

SELECT *
FROM `project.dataset.events`
LIMIT 1000
