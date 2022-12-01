/* {"URI": "basic/events-one-many", "Method": "POST",
   "ResponseBody": {
        "StateValue": "Events"
        }
   } */

SELECT EVENTS.*,
       EVENTS_PERFORMANCE.* /* { "Cardinality": "Many" } */
FROM (SELECT ID, QUANTITY FROM EVENTS) EVENTS
JOIN (SELECT * FROM EVENTS_PERFORMANCE) EVENTS_PERFORMANCE ON EVENTS.ID = EVENTS_PERFORMANCE.EVENT_ID