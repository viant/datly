/* {"URI": "comprehensive/events-many", "Method": "POST", "ResponseBody": {
        "From": "Events"
   },
   "ResponseField": "Data" } */

SELECT events.* /* { "ResponseField": "Data" } */
FROM (SELECT * FROM EVENTS) events
