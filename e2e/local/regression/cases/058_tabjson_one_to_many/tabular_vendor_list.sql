/* {"URI":"tabular/basic/vendors/",  "TabularJSON":{"FloatPrecision":"20"}} */
SELECT vendor.* EXCEPT CREATED,UPDATED,
       products.* EXCEPT VENDOR_ID,CREATED,UPDATED
FROM (SELECT * FROM VENDOR t ) vendor
    JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID
