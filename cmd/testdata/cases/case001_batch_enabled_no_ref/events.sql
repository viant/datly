/* {"Declare":{"EventTypes":"EventTypes"},"ResponseBody":{"From":"EventTypes"}} */

import (
	"testdata/cases/case001_batch_enabled_no_ref.EventTypes"
)


#set($_ = $EventTypes<*EventTypes>(body/))
INSERT INTO event_types(
account_id,
name,
id
) VALUES (
$EventTypes.AccountId,
$EventTypes.Name,
$EventTypes.Id
);

  #foreach($recEvents in $Unsafe.EventTypes.Events)
    #set($recEvents.EventTypeId = $Unsafe.EventTypes.Id)
    $sql.Insert($recEvents, "events");
  #end