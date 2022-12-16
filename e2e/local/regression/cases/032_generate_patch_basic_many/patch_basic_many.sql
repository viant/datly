/* {
   "URI": "basic/foos-many", "Method": "PATCH"
} */

SELECT foos.* /* { "Cardinality": "Many" } */
FROM (SELECT * FROM FOOS) foos
