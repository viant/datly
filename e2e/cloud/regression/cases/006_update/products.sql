/* {
    "URI":"products",
    "Method": "PUT"
 } */

#set($idsIndex = $Products.IndexBy("ID") /*
    { "Required": false, "Util": true }

    SELECT product_id as ID
    FROM products /* { "Connector": "bqdev" } */
    WHERE product_id IN ($Ids)

*/)

#foreach($id in $Ids)
    #if($idsIndex.HasKey($id) == false)
        $logger.Fatal("Product: %v doesn't exist", $id)
    #end
#end

UPDATE products /* {"Connector": "bqdev"} */
SET payment = $payment
WHERE product_id IN ($Ids);