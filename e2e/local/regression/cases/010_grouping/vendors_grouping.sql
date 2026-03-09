/* {"URI":"vendors-grouping/"} */

#set( $_ = $report())
#set( $_ = $Data<?>(output/view).Embed())
#set( $_ = $VendorIDs<[]int>(query/vendorIDs).WithPredicate(0, 'in', 't', 'ID'))

SELECT vendor.*,
       grouping_enabled(vendor),
       allowed_order_by_columns(vendor, 'accountId:ACCOUNT_ID,userCreated:USER_CREATED,totalId:TOTAL_ID,maxId:MAX_ID')
FROM (
    SELECT ACCOUNT_ID,
           USER_CREATED,
           SUM(ID) AS TOTAL_ID,
           MAX(ID) AS MAX_ID
    FROM VENDOR t
    WHERE t.ID IN ($VendorIDs)
    GROUP BY 1, 2
) vendor
