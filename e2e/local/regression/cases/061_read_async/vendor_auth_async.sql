/* {
   "URI":"vendors-async/{vendorID}",
   "Async": {
            "Qualifier": "Jwt.UserID",
            "TableName": "vendors-async"
        }
   } */

#set($_ = $Jwt<string>(Header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Authorization  /*
    {"Type": "Authorizer", "StatusCode": 403}
    SELECT Authorized /* {"DataType":"bool"} */
    FROM (SELECT IS_VENDOR_AUTHORIZED($Jwt.UserID, $vendorID) AS Authorized) t
    WHERE Authorized
*/)

SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT CAST($Jwt.FirstName AS CHAR) AS FIRST_NAME, t.* FROM VENDOR t WHERE t.ID = $vendorID ) vendor
    JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID