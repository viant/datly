/* {"URI":"meta/vendors/"} */
SELECT vendor.* /* {"Style":"Comprehensive", "Field":"Data"}  */,
       products.* EXCEPT VENDOR_ID,
       Meta.* /* {"Kind": "record"} */
FROM (SELECT t.* FROM VENDOR t WHERE 1=1  ) vendor
    JOIN (SELECT * FROM PRODUCT t) products  ON products.VENDOR_ID = vendor.ID
    JOIN (SELECT   CAST(1 + (COUNT(1) / $View.Limit) AS SIGNED)  AS PAGE_CNT, COUNT(1) AS CNT
    FROM ($View.vendor.SQL)  t ) AS Meta ON 1=1
