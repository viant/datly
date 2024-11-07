/* {
   "URI": "basic/events", "Method": "POST"
} */


#set($_ = $Events<?>(body/).Cardinality('One').Tag('anonymous:"true"'))
#set($_ = $Events<?>(body/).Output().Tag('anonymous:"true"'))


SELECT events.*
FROM (SELECT * FROM EVENTS) events
