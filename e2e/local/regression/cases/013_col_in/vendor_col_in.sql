/* {"URI":"col/vendors/"} */

SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT * FROM VENDOR t WHERE t.ID = $VendorID ) vendor
    JOIN (
        SELECT ID,NAME, VENDOR_ID FROM  (

        SELECT ID, NAME, VENDOR_ID  FROM PRODUCT t WHERE 1=1 $View.ColIn("AND","VENDOR_ID")
        UNION ALL
        SELECT ID, NAME, VENDOR_ID FROM PRODUCT t WHERE 1=1 $View.ColIn("AND","VENDOR_ID")
    ) t

    ) products ON products.VENDOR_ID = vendor.ID

