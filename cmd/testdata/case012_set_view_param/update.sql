/* {"URI":"status", "Method":"GET" , "Declare":{"Ids":"[]int", "Event": "struct {Authorized bool} "} } */

#set
($event = $Unsafe.Event /*
    {"Auth": "Jwt", "Kind": "data_view"}
    SELECT (CASE
    WHEN total_events > 0 THEN true
    ELSE false
    END
) as Authorized FROM  (
    SELECT COUNT(*) as total_events FROM events WHERE id = $ID
) T
*/)

#foreach($rec in $Unsafe.Records /*
{"Required": true} SELECT ID, EVENT_TYPE_ID, 'new name' as NEW_NAME FROM events WHERE event_type_id IN($Ids)
    */)

UPDATE event_types
SET name = $rec.NEW_NAME
WHERE ID = $rec.ID;

#end