SELECT events.*,
       eventTypes.*
FROM (
         SELECT COUNT(*)        as totalTypes /*{"DataType": "int"}*/,
                e.id,
                e.event_type_id as eventTypeId,
                et.name         as eventName
         FROM events e
                  JOIN event_types et ON e.event_type_id = et.id
         WHERE 1 = 1
           AND e.quantity > $quantity
           #if($Has.etId && ($Unsafe.etId != 0))
            AND et.name = $eventName
           #end
         #if($Unsafe.filter == 1)
           AND e.id = 10
         #end
         GROUP BY e.event_type_id
         ORDER BY 1
     ) events
         JOIN (
    SELECT *
    FROM event_types
) eventTypes ON events.event_type_id = eventTypes.id

