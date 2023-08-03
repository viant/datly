/* {"URI":"json-codec/"} */

import (
	"regression/cases/042_json_codec.Foo"     as   "com.class.abc.Foo"
	"regression/cases/042_json_codec.Boo"     as   "com.class.abc.Boo"
	"regression/cases/042_json_codec.Bar"     as   "com.class.abc.Bar"
	"regression/cases/042_json_codec.Record"
)

SELECT main.*
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* { "Codec":{ "Ref": "JSON", "OutputType": "$Rec.ClassName" } } */,
             CLASS_NAME as ClassName
         FROM OBJECTS /* { "DataType": "Record" } */
     ) main