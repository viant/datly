/* {"URI":"json-codec"} */

import (
	"regression/cases/042_json_codec.Foo"     as   "java.class.Foo"
	"regression/cases/042_json_codec.Boo"     as   "java.class.Boo"
	"regression/cases/042_json_codec.Bar"     as   "java.class.Bar"
	"regression/cases/042_json_codec.Record"
)

SELECT main.*
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* { "Codec": {"Ref": "JSON", "JSONType": "$Rec.ClassName" } } */,
             CLASS_NAME as ClassName
         FROM OBJECTS /* { "DataType": "Record" } */
     ) main