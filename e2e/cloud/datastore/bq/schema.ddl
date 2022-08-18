
CREATE OR REPLACE TABLE bqdev.product_performance (
    product_id INT64,
    location_id INT,
    timestamp TIMESTAMP,
    quantity FLOAT64,
    price    FLOAT64,
    charge   FLOAT64,
    payment  FLOAT64
) PARTITION BY DATE(timestamp) CLUSTER BY product_id;

