/* {"URI":"inlinejson-codec"} */

import (
	"regression/cases/044_inline_json_codec.Foo"     as   "com.class.Foo"
	"regression/cases/044_inline_json_codec.Boo"     as   "com.class.Boo"
	"regression/cases/044_inline_json_codec.Bar"     as   "com.class.Bar"
	"regression/cases/044_inline_json_codec.Record"
)

SELECT main.*
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* { "Codec": {"Ref": "JSON", "OutputType": "$Rec.ClassName" } } */,
             CLASS_NAME as ClassName
         FROM OBJECTS
     ) main  /* { "DataType": "Record" } */