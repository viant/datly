/* {"URI": "event_types" }*/
SELECT eventTypes.*
FROM (
         SELECT et.*
         FROM event_types et
                  JOIN events e ON et.id = e.event_type_id
         WHERE 1 = 1
           AND et.id = $events.event_type_id
     ) eventTypes
         JOIN (
    SELECT event_type_id, id, quantity, 'timestamp'
    FROM events LIMIT 1
) events ON 1 = 1
