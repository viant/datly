/* {"URI": "basic/events-explicit", "Method": "POST",
   "ResponseBody": {
        "From": "Events"
        }
   } */

SELECT events.* /* { "Cardinality": "One" } */
FROM (SELECT ID, QUANTITY FROM EVENTS) events
