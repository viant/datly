/* {"URI":"inlinejson-codec"} */

import (
	"regression/cases/044_inline_json_codec.Foo"     as   "com.class.abc.Foo"
	"regression/cases/044_inline_json_codec.Boo"     as   "com.class.abc.Boo"
	"regression/cases/044_inline_json_codec.Bar"     as   "com.class.abc.Bar"
	"regression/cases/044_inline_json_codec.Record"
)

#set( $_ = $Data<?>(output/view).Embed())


SELECT main.*,
  cast(main AS Record)
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* { "Codec": {"Ref": "JSON", "OutputType": "$Rec.ClassName" } } */,
             CLASS_NAME as ClassName
         FROM OBJECTS
         WHERE ID != 999
     ) main