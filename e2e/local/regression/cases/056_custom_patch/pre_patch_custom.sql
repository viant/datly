/* {
   "URI": "basic/foos-many-many-custom", "Method": "PATCH"
} */

SELECT foos.*,
       foosPerformance.*
FROM (SELECT * FROM FOOS) foos
JOIN (SELECT * FROM FOOS_PERFORMANCE) foosPerformance on foos.ID = foosPerformance.FOO_ID