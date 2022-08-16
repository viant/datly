/* {"URI":"status", "Method":"GET" , "Declare":{"Ids":"[]int"} } */

#foreach($rec in $Unsafe.Records /*
{"Required": true} SELECT ID, EVENT_TYPE_ID, 'new name' as NEW_NAME FROM events WHERE event_type_id IN($Ids)
    */)

UPDATE event_types
SET name = $rec.NEW_NAME
WHERE ID = $rec.ID;

#end