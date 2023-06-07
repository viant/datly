/* {
   "URI":"performance-async",
   "Async": {
            "Dataset": "${env.Dataset}",
            "Connector": "mysql-dev",
            "BucketURL": "s3://viant-e2e-datly-jobs/"
        }
   } */
SELECT perf.*
FROM ( SELECT
           location_id,
           product_id,
           SUM(quantity) AS quantity,
           AVG(payment) * 1.25 AS price
FROM `bqdev.product_performance` t
GROUP BY 1, 2
) perf
