-- tests/fixtures/select_star.sql
-- Should trigger: select_star (warn)

SELECT *
FROM `project.dataset.orders`
WHERE order_date >= '2024-01-01'
