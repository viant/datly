/* {
   "URI": "basic/events", "Method": "POST", "ResponseBody": {
        "StateValue": "Events"
   }
} */

SELECT events.*
FROM (SELECT * FROM EVENTS) events
