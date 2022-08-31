/*
{ "Cache":{
         "Name": "aerospike",
         "Provider": "aerospike://127.0.0.1:3000/test",
         "Location": "aerospike",
         "TimeToLiveMs": 3600000
         }
}
*/

SELECT events.*,
       eventTypes.*
FROM (
         SELECT
                COUNT(*) as totalTypes,
                id,
                event_type_id as eventTypeId
         FROM events
         WHERE 1 = 1
         AND quantity > $quantity
         GROUP BY event_type_id
         ORDER BY 1
         ) events
JOIN (
    SELECT * FROM event_types
    ) eventTypes /* {"Cache": {"Ref": "aerospike"}, "Warmup": {}, "MetaColumn": "account_id"  } */ ON events.event_type_id = eventTypes.id

