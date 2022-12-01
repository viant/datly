/* {"URI": "basic/events-except", "Method": "POST",
   "ResponseBody": {
        "StateValue": "Events"
        }
 } */

SELECT events.* EXCEPT NAME
FROM (SELECT * FROM EVENTS) events
