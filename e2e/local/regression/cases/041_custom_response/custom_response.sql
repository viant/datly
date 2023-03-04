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
#set($defaultResponse = $http.Do(
    "POST","http://127.0.0.1:8080/", "",
    $http.Defer(),
    $http.OnError($response.CallbackPut("DefaultMessageError", "Default on error message content")),
    $http.OnSuccess($response.CallbackPut("DefaultMessageSuccess", "Default message success"))
))

#set($successResponse = $http.Do(
    "POST","http://127.0.0.1:8080/", "",
    $http.DeferOnSuccess(),
    $http.OnError($response.CallbackPut("SuccessMessageError", "Success on error message content")),
    $http.OnSuccess($response.CallbackPut("SuccessMessageSuccess", "Success message success"))
))

#set($failureResponse = $http.Do(
    "POST","http://127.0.0.1:8080/", "",
    $http.DeferOnFailure(),
    $http.OnError($response.CallbackPut("FailureMessageError", "Failure on error message content")),
    $http.OnSuccess($response.CallbackPut("FailureMessageSuccess", "Failure message success"))
))

#if($Unsafe.Events)
#if($Unsafe.Events.Quantity <= 0)
    $response.Put("CustomField", "Quantity has to be > 0")
    $response.StatusCode(419)
    $response.Failf("CustomError message")
#end

  $sql.Insert($Events, "EVENTS");
#end