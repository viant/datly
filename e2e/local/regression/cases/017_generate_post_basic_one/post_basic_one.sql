/* {
   "URI": "basic/events", "Method": "POST", "ResponseBody": {
        "From": "Events"
   }
} */

SELECT events.*
FROM (SELECT * FROM EVENTS) events
