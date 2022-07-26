/* {"URI": "events/{eventID}" }*/
SELECT events.*
FROM (
         SELECT *
         FROM events e
         WHERE 1 = 1
           AND e.id = $eventID
         ORDER BY 1
     ) events /* {"Selector": {"Constraints": {"Projection": false, "Filterable": []}}} */
