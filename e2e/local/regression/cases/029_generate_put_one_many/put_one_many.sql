/* {   "URI": "basic/foos-one-many", "Method": "PUT"} */

#set($_ = $Foos<?>(body/).Cardinality('One').Tag('anonymous:"true"'))
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true"'))


SELECT foos.*,
       foosPerformance.*
FROM (SELECT * FROM FOOS) foos
JOIN (SELECT * FROM FOOS_PERFORMANCE) foosPerformance  on foos.ID = foosPerformance.FOO_ID