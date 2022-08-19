/* {"URI":"auth/product/{productID}", "Method":"PUT" , "Declare":{"Ids":"[]int", "Authorized":"bool"}} */


#set ($auth = $Unsafe.Auth /*  {"Auth":"Jwt"}  SELECT Authorized  FROM (SELECT  TRUE AS Authorized) t t WHERE Authorized AND $Jwt.UserID > 0  */ )

#foreach($rec in $Unsafe.Records /*
    SELECT ID, STATUS, IS_PRODUCT_AUTHORIZED($Jwt.UserID, ID) AS IS_PRODUCT_AUTHORIZED FROM PRODUCT WHERE ID IN ($Ids)
 */)

#if($rec.IS_PRODUCT_AUTHORIZED == 0)
    $errors.Raise("Unauthorized access to product: $rec.ID")
#end

#if($rec.STATUS == 0)
    $errors.Raise("Changing archived product ID: $rec.ID is not allowed: $rec.ID")
#end

UPDATE PRODUCT
SET STATUS = $Status
WHERE ID = $rec.ID;

$logger.Log("change PRODUCT ID: %v status from: %v to %v\n", $rec.ID, $rec.STATUS, $Unsafe.Status)

INSERT INTO PRODUCT_JN(PRODUCT_ID, USER_ID, OLD_VALUE, NEW_VALUE) VALUES($rec.ID, $rec.STATUS, $Status);

#end





