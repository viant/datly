    /* {"URI":"headers/vendors"} */
SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT * FROM VENDOR t WHERE t.ID = $vendorID /* {"Kind": "header", "Location": "Vendor-Id"} */ ) vendor
JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID