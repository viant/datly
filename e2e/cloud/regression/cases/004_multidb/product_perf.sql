/* {"URI":"products/"} */
SELECT product.* EXCEPT VENDOR_ID
       vendor.*,
       performance.* EXCEPT product_id
FROM (SELECT * FROM PRODUCT t) product
    JOIN (SELECT * FROM VENDOR t WHERE t.ID = $vendorID ) vendor  ON product.VENDOR_ID = vendor.ID
JOIN (SELECT * FROM `bqdev.product_inventory` t) performance /* {"Connector":"bqdev" } */ ON performance.product_id = product.ID
