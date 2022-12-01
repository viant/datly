/* {"URI": "basic/events-many", "Method": "POST", "ResponseBody": {
        "StateValue": "Events"
   }
} */

SELECT events.* /* { "Cardinality": "Many" } */
FROM (SELECT * FROM EVENTS) events
