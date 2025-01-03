/* {"URI": "basic/events-except", "Method": "POST" } */

#set($_ = $Events<?>(body/).Cardinality('One').Tag('anonymous:"true"'))
#set($_ = $Events<?>(body/).Output().Tag('anonymous:"true"'))



SELECT events.* EXCEPT NAME
FROM (SELECT * FROM EVENTS) events
