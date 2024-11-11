/* {"URI":"xml/basic/vendors/", "DataFormat":"xml", "XML":{"FloatPrecision":"20"}} */

#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))


SELECT vendor.* EXCEPT CREATED,UPDATED,
       products.* EXCEPT VENDOR_ID,CREATED,UPDATED
FROM (SELECT * FROM VENDOR t ORDER BY ID) vendor
    LEFT JOIN (SELECT * FROM PRODUCT t ORDER BY ID) products ON products.VENDOR_ID = vendor.ID
