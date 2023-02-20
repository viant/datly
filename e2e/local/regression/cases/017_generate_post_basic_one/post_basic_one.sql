/* {
   "URI": "basic/events", "Method": "POST", "ResponseBody": {
        "From": "Events"
   }
} */

SELECT events.* /* { "Cardinality": "One" } */
FROM (SELECT * FROM EVENTS) events
