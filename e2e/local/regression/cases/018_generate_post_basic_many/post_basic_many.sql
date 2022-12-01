/* {"URI": "basic/events-many", "Method": "POST", "ResponseBody": {
        "From": "Events"
   }
} */

SELECT events.* /* { "Cardinality": "Many" } */
FROM (SELECT * FROM EVENTS) events
