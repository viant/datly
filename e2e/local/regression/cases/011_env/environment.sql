/* { "URI":"vendors-env/", "Const": { "Vendor": "VENDOR", "Product": "PRODUCT" } } */


SELECT vendor.*,
       products.*
FROM (SELECT * FROM $Vendor t WHERE t.ID IN ($vendorIDs) AND 2 = 2) vendor /* { "AllowNulls": true } */
         JOIN (SELECT * FROM $Product t WHERE 2 = 2) products /* { "AllowNulls": true } */ ON products.VENDOR_ID = vendor.ID