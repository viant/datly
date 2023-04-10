/* {"URI":"custom-unmarshall-velty","Method":"PUT","ResponseBody":{"From":"Preference"}} */

import (
	"regression/cases/048_custom_unmarshall_velty.Preference"
	"regression/cases/048_custom_unmarshall_velty.Foo" as "com.class.abc.Foo"
)

#set($_ = $Preference<*Preference>(body/))
#set($_ = $className<string>(query/className))
#set($_ = $Preference.Object /*
        {"TransformKind": "Unmarshal"}
        $decoder.UnmarshalInto($request.QueryParam("className"), true)
    */)

#if($Unsafe.Preference)
UPDATE OBJECTS
SET
    ID = $Preference.Id,
    CLASS_NAME = $className
  #if($Unsafe.Preference.Has.Object == true)
  , OBJECT = $json.Marshal($Preference.Object)
  #end
WHERE ID = $Preference.Id
AND CLASS_NAME = $className;
#end