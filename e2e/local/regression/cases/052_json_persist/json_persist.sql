/* {"URI":"json-persist","Method":"PUT","ResponseBody":{"From":"Preference"}} */

import (
	"regression/cases/052_json_persist.Preference"
)

#set($_ = $Preference<*Preference>(body/))
#set($objectJSON = $json.Marshal($Unsafe.Preference.Object))

UPDATE OBJECTS SET
   OBJECT = $objectJSON
WHERE ID = $Preference.Id
