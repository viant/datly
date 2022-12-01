/* {"URI": "basic/events-except", "Method": "POST",
   "ResponseBody": {
        "From": "Events"
        }
 } */

SELECT events.* EXCEPT NAME
FROM (SELECT * FROM EVENTS) events
