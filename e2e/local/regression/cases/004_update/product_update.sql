/* {"URI":"auth/products/", "Method":"POST", "Authorized":"bool", "IS_AUTH": "bool"}} */


#set($_ = $Ids<[]int>(body/Ids))

#foreach($rec in $Unsafe.Records /*
  {"Auth":"Jwt"}   SELECT ID, STATUS, (IS_PRODUCT_AUTHORIZED($Jwt.UserID, ID)) AS IS_AUTH FROM PRODUCT WHERE ID IN ($Ids)
 */)

#if($rec.IS_AUTH == 0)
    $logger.Fatal("Unauthorized access to product: %v", $rec.ID)
#end

#if($rec.STATUS == 0)
    $logger.Fatal("Changing archived product ID: %v is not allowed: %v", $rec.ID, $rec.ID)
#end


UPDATE PRODUCT
SET STATUS = $Status
WHERE ID = $rec.ID;


$logger.Log("change PRODUCT ID: %v status from: %v to %v\n", $rec.ID, $rec.STATUS, $Unsafe.Status)

INSERT INTO PRODUCT_JN(PRODUCT_ID, USER_ID, OLD_VALUE, NEW_VALUE) VALUES($rec.ID, $Jwt.UserID, $rec.STATUS, $Status);

#end





