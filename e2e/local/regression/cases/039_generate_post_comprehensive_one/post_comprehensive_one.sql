/* {"URI": "comprehensive/events-one", "Method": "POST" } */

#set($_ = $Events<?>(body/Data).Cardinality('One').Tag('anonymous:"true"'))
#set($_ = $Events<?>(body/Data).Output())


SELECT events.*
FROM (SELECT * FROM EVENTS) events
