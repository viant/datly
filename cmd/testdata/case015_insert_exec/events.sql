/* {"URI": "events", "Method": "POST" } */

SELECT events.* /* { "Cardinality": "Many" } */
FROM (SELECT * FROM events) events
