-- tests/fixtures/cross_join.sql
-- Should trigger: cross_join (block)

SELECT
    a.user_id,
    b.product_id
FROM `project.dataset.users` a
CROSS JOIN `project.dataset.products` b
