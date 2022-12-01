/* {"URI": "basic/events-explicit", "Method": "POST",
   "ResponseBody": {
        "StateValue": "Events"
        }
   } */

SELECT events.*
FROM (SELECT ID, QUANTITY FROM EVENTS) events
