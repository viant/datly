/* { "URI":"vendors-env/", "Const": { "Vendor": "VENDOR", "Product": "PRODUCT" } } */
SELECT vendor.*,
       products.*
FROM (SELECT * FROM $Vendor t WHERE t.ID IN ($vendorIDs)) vendor
         JOIN (SELECT * FROM $Product t) products ON products.VENDOR_ID = vendor.ID