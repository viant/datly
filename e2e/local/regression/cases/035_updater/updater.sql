/* {
   "URI": "basic/foos-updater", "Method": "PUT"
} */

SELECT foos.*            /* { "Cardinality": "Many" } */
FROM (SELECT * FROM FOOS) foos
