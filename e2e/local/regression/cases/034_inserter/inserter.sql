/* {
   "URI": "basic/foos-inserter", "Method": "POST"
} */

SELECT foos.*            /* { "Cardinality": "Many" } */
FROM (SELECT * FROM FOOS) foos /* { "ExecKind": "service" } */
