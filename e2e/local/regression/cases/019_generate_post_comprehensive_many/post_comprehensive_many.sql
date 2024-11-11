/* {"URI": "comprehensive/events-many", "Method": "POST" } */


#set($_ = $Events<?>(body/Data).Cardinality('Many'))
#set($_ = $Status<?>(output/status).Tag('anonymous:"true"'))
#set($_ = $Data<?>(body/Data).Output())


SELECT events.*
FROM (SELECT * FROM EVENTS) events
