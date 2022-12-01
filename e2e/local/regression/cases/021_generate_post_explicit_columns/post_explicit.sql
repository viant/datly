/* {"URI": "basic/events-explicit", "Method": "POST",
   "ResponseBody": {
        "From": "Events"
        }
   } */

SELECT events.*
FROM (SELECT ID, QUANTITY FROM EVENTS) events
