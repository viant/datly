/* {"URI":"performance/"} */
SELECT perf.*
FROM ( SELECT
           location_id,
           product_id,
           SUM(quantity) AS quantity,
           AVG(payment) * 1.25 AS price
FROM `bqdev.product_performance` t ) perf