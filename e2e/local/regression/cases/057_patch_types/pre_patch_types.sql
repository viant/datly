/* {
   "URI": "basic/patch-types", "Method": "PATCH"
} */

#set($_ = $Foos<?>(body/).Cardinality('Many').Tag('anonymous:"true"'))
#set($_ = $Foos<?>(body/).Output().Cardinality('Many').Tag('anonymous:"true"'))


SELECT foos.*,
       foosPerformance.*
FROM (SELECT * FROM FOOS) foos
JOIN (SELECT * FROM FOOS_PERFORMANCE) foosPerformance on foos.ID = foosPerformance.FOO_ID
