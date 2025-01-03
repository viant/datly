/* {"URI":"tabular/basic/vendors/", "DataFormat":"tabular", "TabularJSON":{"FloatPrecision":"20"}} */

#set( $_ = $Data<?>(output/view).Embed())

SELECT vendor.* EXCEPT CREATED,UPDATED,
       products.* EXCEPT VENDOR_ID,CREATED,UPDATED
FROM (SELECT * FROM VENDOR t ) vendor
    LEFT JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID
