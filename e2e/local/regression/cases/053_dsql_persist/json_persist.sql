/* {"URI":"json-persist","Method":"PUT","ResponseBody":{"From":"Preference"}} */

import (
	"regression/cases/052_json_persist.Preference"
)

#set($_ = $Preference<*Preference>(body/))
#set($objectJSON = $json.Marshal($Unsafe.Preference.Object))


#set($_ = $Unsafe.Record /* {"Required":false}
SELECT ID as Id, OBJECT as obj FROM OBJECTS /* {"Selector":{}} */ WHERE ID = $Preference.Id
*/)


#if(!$Unsafe.Record)
     $response.StatusCode(419)
    $response.Failf("CustomError message")
#end


UPDATE OBJECTS SET
   OBJECT = $objectJSON
WHERE ID = $Preference.Id
