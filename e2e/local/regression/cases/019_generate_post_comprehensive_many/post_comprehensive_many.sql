/* {"URI": "comprehensive/events-many", "Method": "POST", "ResponseBody": {
        "From": "Events"
   },
   "Field": "Data" } */

SELECT events.* /* { "Field": "Data" } */
FROM (SELECT * FROM EVENTS) events
