/* {
   "URI": "basic/foos-many-many", "Method": "PATCH"
} */

SELECT foos.*            /* { "Cardinality": "Many" } */,
       foosPerformance.* /* { "Cardinality": "Many" } */
FROM (SELECT * FROM FOOS) foos
JOIN (SELECT * FROM FOOS_PERFORMANCE) foosPerformance on foos.ID = foosPerformance.FOO_ID
