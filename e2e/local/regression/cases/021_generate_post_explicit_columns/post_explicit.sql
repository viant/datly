/* {"URI": "basic/events-explicit", "Method": "POST", "ReturnBody": true } */

SELECT events.*
FROM (SELECT ID, QUANTITY FROM EVENTS) events
