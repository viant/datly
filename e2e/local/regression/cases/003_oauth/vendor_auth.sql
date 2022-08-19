/* {"URI":"auth/vendors/{vendorID}"} */
SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT '$Unsafe.Jwt.FirstName' AS FIRST_NAME, t.* FROM VENDOR t WHERE t.ID = $vendorID AND $Auth.Authorized ) vendor
JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID
JOIN(
    SELECT Authorized /* {"DataType":"bool"} */
    FROM (SELECT IS_VENDOR_AUTHORIZED($Jwt.UserID, $vendorID) AS Authorized) t
    WHERE Authorized
) Auth /* {"Auth":"Jwt"} */ ON 1=1