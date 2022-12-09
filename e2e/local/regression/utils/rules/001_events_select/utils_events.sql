/* {"Method": "GET", "URI": "events/{ID}" } */
SELECT events.* /* {"Style":"Comprehensive", "ResponseField":"Data"}  */
FROM ( SELECT * FROM EVENTS WHERE ID = $ID ) events
