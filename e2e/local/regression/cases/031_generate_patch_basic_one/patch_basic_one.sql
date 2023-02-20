/* {
   "URI": "basic/foos", "Method": "PATCH"
} */

SELECT foos.* /* { "Cardinality": "One" } */
FROM (SELECT * FROM FOOS) foos
