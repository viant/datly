    /* {"URI":"vendors/{vendorID}"} */
SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT * FROM VENDOR t WHERE t.ID = $vendorID ) vendor
JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID