/* {"URI":"json-persist","Method":"PUT","ResponseBody":{"From":"Preference"}} */

import (
	"regression/cases/052_json_persist.Preference"
)

#set($_ = $Preference<*Preference>(body/))


UPDATE OBJECTS SET
   OBJECT = '$json.Marshal($Unsafe.Preference.Object)'
WHERE ID = $Preference.Id
