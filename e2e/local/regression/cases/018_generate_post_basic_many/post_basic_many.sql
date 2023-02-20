/* {"URI": "basic/events-many", "Method": "POST", "ResponseBody": {
        "From": "Events"
   }
} */

SELECT events.*
FROM (SELECT * FROM EVENTS) events
