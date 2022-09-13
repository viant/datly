/* {"URI":"vendors-env/" } */
SELECT vendor.*
FROM (SELECT * FROM $TableName /* { "Kind": "env" } */ t WHERE t.ID IN ($vendorIDs)) vendor