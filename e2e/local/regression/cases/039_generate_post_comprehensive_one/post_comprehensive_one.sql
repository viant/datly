/* {"URI": "comprehensive/events-one", "Method": "POST" } */

#set($_ = $Events<?>(body/Data).Cardinality('Many').Tag('anonymous:"true"'))
#set($_ = $Events<?>(body/Data).Output())


SELECT events.* /* { "Cardinality": "One" } */
FROM (SELECT * FROM EVENTS) events
