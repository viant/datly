/* {"URI": "comprehensive/events-many", "Method": "POST", "ResponseBody": {
        "StateValue": "Events"
   },
   "ResponseField": "Data" } */

SELECT events.* /* { "ResponseField": "Data", "Cardinality": "Many" } */
FROM (SELECT * FROM EVENTS) events
