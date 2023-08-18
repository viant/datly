/* {
   "URI": "boos", "Method": "GET"
} */

import (
    "boos.BoosQueryBuilder"
)

#set($_ = $Criteria<?>(http_request/).WithCodec("CriteriaBuilder", "*BoosQueryBuilder").Selector())

SELECT boos.*
FROM (SELECT * FROM BOOS) boos /* { "CriteriaParam": "Criteria" } */
