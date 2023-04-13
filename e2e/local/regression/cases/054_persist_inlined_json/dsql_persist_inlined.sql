/* {"URI":"dsql-persist-inlined","Method":"PUT","ResponseBody":{"From":"Preference"}} */

import (
    "regression/cases/054_persist_inlined_json.Preference"
)

#set($_ = $Preference<*Preference>(body/))


#set($_ = $Record /* {"Required":false}
SELECT ID as Id, OBJECT AS obj, '' AS Abc FROM OBJECTS /* {"Selector":{}} */ WHERE ID = $Preference.Id
*/)

$fmt.Printf("has previous record: %T %v\n", $Unsafe.Record, $Unsafe.Record)

#if($Unsafe.Record.Id == 0)
     $response.StatusCode(419)
    $response.Failf("CustomError message")
#end


$sql.Update($Preference, "OBJECTS")