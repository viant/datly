/* {"URI": "basic/events-one-one", "Method": "POST",
   "ResponseBody": {
        "From": "Events"
   }
} */

SELECT EVENTS.* /* { "Cardinality": "One" } */,
       EVENTS_PERFORMANCE.* /* { "Cardinality": "One" } */
FROM (SELECT ID, QUANTITY FROM EVENTS) EVENTS
JOIN (SELECT * FROM EVENTS_PERFORMANCE) EVENTS_PERFORMANCE ON EVENTS.ID = EVENTS_PERFORMANCE.EVENT_ID