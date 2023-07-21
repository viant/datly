/* {
   "URI": "boos", "Method": "GET"
} */

import (
    "boos.BoosQueryBuilder"
)

#set($_ = $CriteriaBuilder<?>(http_request/) /* { "HandlerType": "*BoosQueryBuilder" } */)

SELECT boos.*
FROM (SELECT * FROM BOOS) boos /* { "CriteriaParam": "CriteriaBuilder" } */
