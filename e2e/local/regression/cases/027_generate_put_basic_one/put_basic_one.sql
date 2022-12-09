/* {
   "URI": "basic/foos", "Method": "PUT", "ResponseBody": {
        "From": "Foos"
   }
} */

SELECT foos.*
FROM (SELECT * FROM FOOS) foos
