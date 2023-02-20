/* {
   "URI": "basic/foos-many", "Method": "PUT", "ResponseBody": {
        "From": "Foos"
   }
} */

SELECT foos.*
FROM (SELECT * FROM FOOS) foos
