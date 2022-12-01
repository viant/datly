/* {"URI": "comprehensive/events-many", "Method": "POST", "ResponseBody": {
        "From": "Events"
   },
   "ResponseField": "Data" } */

SELECT events.* /* { "ResponseField": "Data", "Cardinality": "Many" } */
FROM (SELECT * FROM EVENTS) events
