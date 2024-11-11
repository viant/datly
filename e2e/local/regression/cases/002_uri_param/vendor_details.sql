/* {"URI":"vendors/{vendorID}", "DocURL":"./doc.yaml" } */


#set( $_ = $Fields<[]string>(query/fields).Optional().QuerySelector('vendor'))
#set( $_ = $Page<int>(query/page).Optional().QuerySelector('vendor'))
#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))


SELECT wrapper.* EXCEPT ID,
       vendor.*,
       products.* EXCEPT VENDOR_ID,
       setting.* EXCEPT ID
FROM (SELECT ID FROM VENDOR WHERE  ID = $vendorID ) wrapper
JOIN (SELECT * FROM VENDOR t WHERE t.ID = $vendorID ) vendor ON vendor.ID = wrapper.ID AND 1=1
JOIN (SELECT * FROM (SELECT (1) AS IS_ACTIVE, (3) AS CHANNEL, CAST($vendorID AS SIGNED) AS ID) t ) setting ON setting.ID = wrapper.ID
JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID