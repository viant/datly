/* {"URI": "comprehensive/events-many", "Method": "POST", "ReturnBody": true, "ResponseField": "Data" } */

SELECT events.* /* { "ResponseField": "Data", "Cardinality": "Many" } */
FROM (SELECT * FROM EVENTS) events
