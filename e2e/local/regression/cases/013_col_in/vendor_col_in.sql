/* {"URI":"col/vendors/",
    "Cache":{
         "Name": "aerospike",
         "Provider": "aerospike://127.0.0.1:3000/test",
         "Location": "${view.Name}",
         "TimeToLiveMs": 3600000
    }
 } */

#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))


SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT * FROM VENDOR t ) vendor
    JOIN (
        SELECT ID,NAME, VENDOR_ID FROM  (

        SELECT ID, NAME, VENDOR_ID  FROM PRODUCT t WHERE 1=1 $View.ParentJoinOn("AND","VENDOR_ID")
        UNION ALL
        SELECT ID, NAME, VENDOR_ID FROM PRODUCT t WHERE 1=1 $View.ParentJoinOn("AND","VENDOR_ID")
    ) t WHERE 1 = 1


    ) products /* { "Cache":{"Ref":"aerospike"}, "Warmup":{"":[""]}} */ ON products.VENDOR_ID = vendor.ID

