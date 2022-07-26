/* {"URI": "events/{eventID}" }*/
SELECT events.*
FROM (
         SELECT *
         FROM events e
         WHERE 1 = 1
           AND e.id = $eventID
         ORDER BY 1
     ) events /* {"Cache": {"Provider": "aerospike://127.0.0.1:3000/test", "Location": "${view.Name}", "TimeToLiveMs":10240}} */
