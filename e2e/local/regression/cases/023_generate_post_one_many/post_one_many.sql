/* {"URI": "basic/events-one-many", "Method": "POST",
   "ResponseBody": {
        "From": "Events"
        }
   } */

SELECT EVENTS.* /* { "Cardinality": "One" } */,
       EVENTS_PERFORMANCE.*
FROM (SELECT ID, QUANTITY FROM EVENTS) EVENTS
JOIN (SELECT * FROM EVENTS_PERFORMANCE) EVENTS_PERFORMANCE ON EVENTS.ID = EVENTS_PERFORMANCE.EVENT_ID