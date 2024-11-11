/* {"URI": "basic/events-one-many", "Method": "POST" } */

#set($_ = $Events<?>(body/).Cardinality('One').Tag('anonymous:"true"'))
#set($_ = $Events<?>(body/).Output().Tag('anonymous:"true"'))


SELECT EVENTS.*,
       EVENTS_PERFORMANCE.*
FROM (SELECT ID, QUANTITY FROM EVENTS) EVENTS
JOIN (SELECT * FROM EVENTS_PERFORMANCE) EVENTS_PERFORMANCE ON EVENTS.ID = EVENTS_PERFORMANCE.EVENT_ID