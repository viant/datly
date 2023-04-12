/* {"URI":"dsql-persist","Method":"PUT","ResponseBody":{"From":"Preference"}} */

import (
	"regression/cases/053_dsql_persist.Preference"
)

#set($_ = $Preference<*Preference>(body/))


#set($_ = $Record /* {"Required":false}
SELECT ID as Id, OBJECT AS obj, '' AS Abc FROM OBJECTS /* {"Selector":{}} */ WHERE ID = $Preference.Id
*/)

$fmt.Printf("has previous record: %T %v\n", $Unsafe.Records, $Unsafe.Records)

#set($objectJSON = $json.Marshal($Unsafe.Preference.Object))
#if($Unsafe.Record.Id == 0)
     $response.StatusCode(419)
    $response.Failf("CustomError message")
#end



UPDATE OBJECTS SET
   OBJECT = $objectJSON
WHERE ID = $Preference.Id
