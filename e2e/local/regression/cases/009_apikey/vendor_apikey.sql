/* {
       "URI":"secured/vendors/{vendorID}",
       "Cache":{
         "Name": "aerospike",
         "Provider": "aerospike://127.0.0.1:3000/test",
         "Location": "${view.Name}",
         "TimeToLiveMs": 3600000
    }
} */

#set( $_ = $Page<int>(query/page).Optional().QuerySelector('vendor'))
#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))


SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT * FROM VENDOR t WHERE t.ID = $vendorID ) vendor
JOIN (SELECT * FROM PRODUCT t WHERE  1=1 #if($vendorID < 0) AND 1=2 #end) products /* {"Cache":{"Ref":"aerospike"}, "Warmup":{"vendorID":[1,2,3,4,5]}} */  ON products.VENDOR_ID = vendor.ID