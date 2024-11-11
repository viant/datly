/* { "URI": "basic/foos", "Method": "PUT" } */

#set($_ = $Foos<?>(body/).Cardinality('One').Tag('anonymous:"true"'))
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true"'))


SELECT foos.*
FROM (SELECT * FROM FOOS) foos
