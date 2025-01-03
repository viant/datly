/* {"URI": "basic/events-many", "Method": "POST"
} */

#set($_ = $Events<?>(body/).Cardinality('Many').Tag('anonymous:"true"'))
#set($_ = $Events<?>(body/).Output().Tag('anonymous:"true"'))


SELECT events.*
FROM (SELECT * FROM EVENTS) events
