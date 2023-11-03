/* {"URI":"raw_json"} */

import (
	"regression/cases/045_raw_json.Record"
)

SELECT main.*,
       cast(main AS Record)
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences,
             CLASS_NAME as ClassName
         FROM OBJECTS
         WHERE ID != 999
     ) main