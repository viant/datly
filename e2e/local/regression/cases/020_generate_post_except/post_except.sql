/* {"URI": "basic/events-except", "Method": "POST", "ReturnBody": true } */

SELECT events.* EXCEPT NAME
FROM (SELECT * FROM EVENTS) events
