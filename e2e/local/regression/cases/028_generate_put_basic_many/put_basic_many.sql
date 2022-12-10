/* {
   "URI": "basic/foos-many", "Method": "PUT", "ResponseBody": {
        "From": "Foos"
   }
} */

SELECT foos.* /* { "Cardinality": "Many" } */
FROM (SELECT * FROM FOOS) foos
