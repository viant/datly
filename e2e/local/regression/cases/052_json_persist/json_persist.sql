/* {"URI":"json-persist","Method":"PUT"} */

import (
	"regression/cases/052_json_persist.Preference"
)

#set( $_ = $Preference<*Preference>(body/))
#set( $_ = $Preference<*Preference>(body/).Tag('anonymous:"true"').Output())


UPDATE OBJECTS SET
   OBJECT = $json.Marshal($Unsafe.Preference.Object)
WHERE ID = $Preference.Id
