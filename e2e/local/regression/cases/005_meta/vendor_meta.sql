/* {"URI":"meta/vendors/"} */
SELECT vendor.*,
       products.* EXCEPT VENDOR_ID,
       Meta.*   
FROM (SELECT t.* FROM VENDOR t WHERE 1=1  ) vendor
    JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID
    JOIN (SELECT 1 + (COUNT(1) / $View.Limit) AS PAGE_CNT, COUNT(1) AS TOTAL_COUNT
                 FROM ($View.vendor.SQL)  t ) AS Meta ON 1=1
