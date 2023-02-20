/* {
   "URI": "basic/foos-inserter", "Method": "POST"
} */

SELECT foos.*
FROM (SELECT * FROM FOOS) foos /* { "ExecKind": "service" } */
