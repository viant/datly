/* {
   "URI": "basic/foos-differ", "Method": "PATCH"
} */

SELECT foos.*                   /* { "Cardinality": "Many" } */
FROM (SELECT * FROM FOOS) foos  /* { "ExecKind": "service", "FetchRecords": true } */
