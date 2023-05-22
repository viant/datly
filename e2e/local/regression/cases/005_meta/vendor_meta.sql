/* {"URI":"meta/vendors/", "CaseFormat":"lc"} */
SELECT vendor.* /* {"Style":"Comprehensive", "Field":"Data"}  */,
       products.* EXCEPT VENDOR_ID,
       Meta.* /* {"Kind": "record"} */
FROM (SELECT t.* FROM VENDOR t WHERE 1=1  ) vendor
    JOIN (SELECT ID, NAME AS my_name /* {"IgnoreCaseFormatter":true} */, VENDOR_ID   FROM PRODUCT t) products  ON products.VENDOR_ID = vendor.ID
    JOIN (SELECT   CAST(1 + (COUNT(1) / $View.Limit) AS SIGNED)  AS PAGE_CNT, COUNT(1) AS CNT
    FROM ($View.vendor.SQL)  t ) AS Meta ON 1=1
