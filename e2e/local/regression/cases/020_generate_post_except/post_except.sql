/* {"URI": "basic/events-except", "Method": "POST",
   "ResponseBody": {
        "From": "Events"
        }
 } */

SELECT events.* EXCEPT NAME /* { "Cardinality": "One" } */
FROM (SELECT * FROM EVENTS) events
