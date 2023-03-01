/* {"URI": "comprehensive/events-one", "Method": "POST", "ResponseBody": {
        "From": "Events"
   },
   "Field": "Data" } */

SELECT events.* /* { "Cardinality": "One" } */
FROM (SELECT * FROM EVENTS) events
