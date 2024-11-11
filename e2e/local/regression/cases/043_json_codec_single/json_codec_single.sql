/* {"URI":"json-codec/{Id}"} */

import (
	"regression/cases/043_json_codec_single.Foo"     as   "com.class.abc.Foo"
	"regression/cases/043_json_codec_single.Boo"     as   "com.class.abc.Boo"
	"regression/cases/043_json_codec_single.Bar"     as   "com.class.abc.Bar"
	"regression/cases/043_json_codec_single.Record"
)

#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))


SELECT main.* EXCEPT(Id),
cast(main AS Record),
cardinality(main, 'One') AS main
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* { "Codec": {"Ref": "JSON", "OutputType": "$Rec.ClassName" } } */,
             CLASS_NAME as ClassName
         FROM OBJECTS
         WHERE ID = $Id
     ) main