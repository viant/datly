/* {"URI":"vendors/"} */
SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT * FROM VENDOR t ) vendor
JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID
