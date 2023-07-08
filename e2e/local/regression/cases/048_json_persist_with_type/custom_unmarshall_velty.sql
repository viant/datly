/* {"URI":"custom-unmarshall-velty","Method":"PUT","ResponseBody":{"From":"Preference"}} */

import (
	"regression/cases/048_json_persist_with_type.Preference"
	"regression/cases/048_json_persist_with_type.Foo" as "com.class.abc.Foo"
)

#set($_ = $Preference<*Preference>(body/))
#set($_ = $Preference.Object /*
        {"TransformKind": "Unmarshal"}
        $decoder.UnmarshalInto($request.QueryParam("className"), true)
    */)
#set($_ = $className<string>(query/className))


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