/* {"URI":"raw_json"} */

import (
	"regression/cases/045_raw_json.Record"
)

SELECT main.*
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences,
             CLASS_NAME as ClassName
         FROM OBJECTS
     ) main  /* { "DataType": "Record" } */