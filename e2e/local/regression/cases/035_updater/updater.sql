/* {
   "URI": "basic/foos-updater", "Method": "PUT"
} */

#set($_ = $Foos<?>(body/).Cardinality('Many').Tag('anonymous:"true"'))
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true"'))


SELECT foos.*
FROM (SELECT * FROM FOOS) foos
