/* {
   "URI":"basic/events-validator",
   "Method":"POST",
   "Declare":{"Events":"Events"},
   "RequestBody":{"ReturnAsResponse":true},
   "TypeSrc":{
        "URL":"e2e/local/regression/cases/024_validator",
        "Types":["*Events"]
        }
   } */

$sequencer.Allocate("EVENTS", $Events, "Id")
$sequencer.Allocate("EVENTS_PERFORMANCE", $Events, "EventsPerformance/Id")

#set($eTypes = $Unsafe.EventTypes /* { "Codec": "structql", "DataType": "Events", "Target": "" }
    SELECT Price, Timestamp FROM `EventsPerformance`
*/)

#set($validationResult = $http.Do("POST", "http://localhost:8081/dev/validate/event-perf", $eTypes))
#if($validationResult.Invalid)
$logger.Fatal("%v", $validationResult.Message)
#end

INSERT INTO EVENTS (
ID,
QUANTITY
) VALUES (
$Events.Id /* {"DataType":"Events","Target":"","Cardinality":"One"} */ ,
$Events.Quantity
);

	#foreach($recEventsPerformance in $Unsafe.Events.EventsPerformance)
	#set($recEventsPerformance.EventId = $Unsafe.Events.Id)
	INSERT INTO EVENTS_PERFORMANCE (
	ID,
	PRICE,
	EVENT_ID,
	TIMESTAMP
	) VALUES (
	$recEventsPerformance.Id,
	$recEventsPerformance.Price,
	$recEventsPerformance.EventId,
	$recEventsPerformance.Timestamp
	);
	
	#end