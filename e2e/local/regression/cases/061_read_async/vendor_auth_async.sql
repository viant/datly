/*
{
   "URI":"async/vendor/{vendorID}",
   "Cache":{
         "Name": "aerospike",
         "Provider": "aerospike://127.0.0.1:3000/test",
         "Location": "${View.Name}",
         "TimeToLiveMs": 3600000
    },
    "Async":{
        "Destination": "file:///tmp/jobs/datly"
   }
}
*/



#set($_ = $UserID<string>(state/Jwt/UserID).Async())
#set($_ = $UserEmail<string>(state/Jwt/Email).Async())
#set($_ = $JobMatchKey<string>(query/).Async())


#set($_ = $Job<?>(async/job).Output().WithTag('json:",omitempty"'))
#set($_ = $Result<?>(output/view))
#set($_ = $Status<?>(output/status))


#set($_ = $Jwt<string>(Header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Authorization  /*
    {"Type": "Authorizer", "StatusCode": 403}
    SELECT Authorized /* {"DataType":"bool"} */
    FROM (SELECT IS_VENDOR_AUTHORIZED($Jwt.UserID, $vendorID) AS Authorized) t
    WHERE Authorized
*/)

SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT CAST($Jwt.FirstName AS CHAR) AS FIRST_NAME, t.* FROM VENDOR t  WHERE t.ID = $vendorID ) vendor /* {"Cache":{"Ref":"aerospike"}} */
    JOIN (SELECT * FROM PRODUCT t) products /* {"Cache":{"Ref":"aerospike"}} */  ON products.VENDOR_ID = vendor.ID