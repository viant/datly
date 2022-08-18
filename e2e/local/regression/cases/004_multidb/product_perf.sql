/* {"URI":"vendors/{vendorID}"} */
SELECT products.* EXCEPT VENDOR_ID
       vendor.*,
       performance.* EXCEPT product_id
FROM (SELECT * FROM VENDOR t WHERE t.ID = $vendorID ) vendor
JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID
    JOIN (SELECT * FROM PRODUCT t) products /* {"Connector":"bqdev" } */ ON products.VENDOR_ID = vendor.ID
