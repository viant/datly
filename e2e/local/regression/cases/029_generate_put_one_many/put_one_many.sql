/* {
   "URI": "basic/foos-one-many", "Method": "PUT", "ResponseBody": {
        "From": "Foos"
   }
} */

SELECT foos.* /* { "Cardinality": "One" } */,
       foosPerformance.*
FROM (SELECT * FROM FOOS) foos
JOIN (SELECT * FROM FOOS_PERFORMANCE) foosPerformance  on foos.ID = foosPerformance.FOO_ID