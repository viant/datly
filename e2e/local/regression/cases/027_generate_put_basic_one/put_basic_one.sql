/* {
   "URI": "basic/foos", "Method": "PUT", "ResponseBody": {
        "From": "Foos"
   }
} */

SELECT foos.* /* { "Cardinality": "One" } */
FROM (SELECT * FROM FOOS) foos
