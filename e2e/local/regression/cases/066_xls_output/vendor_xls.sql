/* {"URI":"vendors/xls/", "DataFormat":"xls"} */

#set( $_ = $Data<?>(output/view).Embed())


SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT * FROM VENDOR t ) vendor
    JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID
