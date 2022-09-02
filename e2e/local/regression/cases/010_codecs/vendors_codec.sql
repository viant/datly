/* {"URI":"vendors-codec/"} */
SELECT vendor.*
FROM (SELECT * FROM VENDOR t WHERE t.ID IN ($vendorIDs) ) vendor