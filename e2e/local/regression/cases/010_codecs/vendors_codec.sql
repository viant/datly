/* {"URI":"vendors-codec/"} */

#set( $_ = $Data<?>(output/view).Embed())

SELECT vendor.*
FROM (SELECT * FROM VENDOR t WHERE t.ID IN ($vendorIDs) ) vendor