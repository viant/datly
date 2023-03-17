/* {"URI":"json-codec/{Id}"} */

import (
	"regression/cases/043_json_codec_single.Foo"     as   "com.class.Foo"
	"regression/cases/043_json_codec_single.Boo"     as   "com.class.Boo"
	"regression/cases/043_json_codec_single.Bar"     as   "com.class.Bar"
	"regression/cases/043_json_codec_single.Record"
)

SELECT main.* EXCEPT Id /* {"Cardinality":"One"} */
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* { "Codec": {"Ref": "JSON", "OutputType": "$Rec.ClassName" } } */,
             CLASS_NAME as ClassName
         FROM OBJECTS /* { "DataType": "Record" } */
         WHERE ID = $Id
     ) main