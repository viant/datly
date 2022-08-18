/* {"URI":"status", "Method":"GET"} */

SELECT events.* FROM (SELECT * FROM events WHERE ID IN ($Ids /* {"Codec": "AsInts"} */)) as events