/* {"URI":"meta/vendors-format/"} */
SELECT vendor.* /* {"Style":"Comprehensive", "ResponseField":"Data"}  */,
       products.* EXCEPT VENDOR_ID,
       Meta.* /* {"Kind": "record"} */,
       products_meta.* EXCEPT VENDOR_ID /* {"Kind": "record"} */
FROM (SELECT t.* FROM VENDOR t WHERE 1=1  ) vendor
    JOIN (SELECT * FROM PRODUCT t) products /* { "AllowNulls": true } */
    ON products.VENDOR_ID = vendor.ID
    JOIN (SELECT   CAST(1 + (COUNT(1) / $View.Limit) AS SIGNED)  AS PAGE_CNT, COUNT(1) AS CNT
    FROM ($View.vendor.SQL)  t ) AS Meta ON 1=1
    JOIN (
        SELECT VENDOR_ID, CAST(1 + (COUNT(1) / $View.Limit) AS SIGNED) AS PAGE_CNT, COUNT(*) AS TOTAL_PRODUCTS FROM ($View.products.SQL) PROD_META GROUP BY VENDOR_ID
    ) AS products_meta ON 1=1
