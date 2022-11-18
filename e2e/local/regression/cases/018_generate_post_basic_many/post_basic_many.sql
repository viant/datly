/* {"URI": "basic/events-many", "Method": "POST", "ReturnBody": true } */

SELECT events.* /* { "Cardinality": "Many" } */
FROM (SELECT * FROM EVENTS) events
