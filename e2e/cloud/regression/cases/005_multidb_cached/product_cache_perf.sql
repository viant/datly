/* {"URI":"cached/products/",
    "Cache":{
         "Name": "aerospike",
         "Provider": "aerospike://127.0.0.1:3000/test",
         "Location": "${view.Name}",
         "TimeToLiveMs": 3600000
    }
  } */
SELECT product.* EXCEPT VENDOR_ID,
       vendor.*,
       performance.* EXCEPT product_id
FROM (SELECT * FROM PRODUCT t) product
    JOIN (SELECT * FROM VENDOR t ) vendor  ON product.VENDOR_ID = vendor.ID
JOIN ( SELECT
    location_id,
    product_id,
    SUM(quantity) AS quantity,
    AVG(payment) * 1.25 AS price
    FROM $Db[bqdev]`viant-e2e.bqdev.product_performance` t
    WHERE 1=1
    #if($Unsafe.period == "today")
        AND 1 = 1
    #end
    GROUP BY 1, 2) performance /*  {"Connector":"bqdev", "Cache":{"Ref":"aerospike"}, "Warmup":{"period":["today"]}} */ ON performance.product_id = product.ID
