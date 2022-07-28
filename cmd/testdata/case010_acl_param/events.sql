/* {"URI": "events/{eventID}" }*/
SELECT eventTypes.*,
       events.*
FROM (
         SELECT *
         FROM eventTypes et
         JOIN events e ON et.id = e.event_type_id
         WHERE 1 = 1
           AND et.id = $events.EventTypeID
     ) eventTypes
JOIN (
        SELECT
               * FROM events
    ) events /*  */ ON eventTypes.id = events.event_type_id
