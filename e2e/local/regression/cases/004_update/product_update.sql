/* {"URI":"auth/product/{productID}", "Method":"PUT" , "Declare":{"Ids":"[]int", "Authorized":"bool"}} */



#foreach($rec in $Unsafe.Records /*
  {"Auth":"Jwt"}   SELECT ID, STATUS, IS_PRODUCT_AUTHORIZED($Jwt.UserID, ID) AS IS_AUTH FROM PRODUCT WHERE ID IN ($Ids)
 */)

#if($rec.IS_AUTH == 0)
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





