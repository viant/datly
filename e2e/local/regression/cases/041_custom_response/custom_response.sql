/* {"URI": "comprehensive/custom-response", "Method": "POST", "ResponseBody": {
        "From": "Events"
   },
   "Field": "Data" } */


import (
	"regression/cases/041_custom_response.Events"
	"regression/cases/041_custom_response.Data"
)

#set($_ = $Events<*Events>(body/Data))

$sequencer.Allocate("EVENTS", $Events, "Id")
#if($Unsafe.Events)
#if($Unsafe.Events.Quantity <= 0)
    $response.Add("CustomField", "Quantity has to be > 0")
    $response.FailfWithStatusCode(419,"CustomError message")
#end

  $sql.Insert($Events, "EVENTS");
#end