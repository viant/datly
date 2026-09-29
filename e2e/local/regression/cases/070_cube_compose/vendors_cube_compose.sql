/* {"URI":"vendors-cube-compose/","Name":"vendors cube compose","MCPTool":true} */

#set( $_ = $cube())
#set( $_ = $cubeCompose(true))
#set( $_ = $Data<?>(output/view).Embed())
#set( $_ = $VendorIDs<[]int>(query/vendorIDs).WithPredicate(0, 'in', 't', 'ID'))

SELECT vendor.*,
       grouping_enabled(vendor),
       allowed_order_by_columns(vendor, 'accountId:ACCOUNT_ID,totalId:TOTAL_ID')
FROM (
    SELECT ACCOUNT_ID,
           SUM(ID) AS TOTAL_ID
    FROM VENDOR t
    WHERE t.ID IN ($VendorIDs)
    GROUP BY 1
) vendor
