-- tests/fixtures/clean_query.sql
-- Should produce zero findings

SELECT
    order_id,
    customer_id,
    total_amount,
    order_date
FROM `project.dataset.orders`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
