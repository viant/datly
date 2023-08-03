/* {
   "URI": "boos", "Method": "GET"
} */

import (
    "boos.BoosQueryBuilder"
)

#set($_ = $Criteria<?>(http_request/).WithCodec("CriteriaBuilder", "*BoosQueryBuilder").Implicit())

SELECT boos.*
FROM (SELECT * FROM BOOS) boos /* { "CriteriaParam": "Criteria" } */
