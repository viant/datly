/* {
   "URI":"basic/events-validator",
   "Method":"POST"
   } */

import (
    "regression/cases/024_validator_codec.Events"
)


#set($_ = $Events<*Events>(body/))
#set($_ = $Events<*Events>(body/).Output().Tag('anonymous:"true"'))

#set($_  = $EventTypes<?>(param/Events) /*
     SELECT Price, Timestamp FROM `/EventsPerformance`
*/)

$sequencer.Allocate("EVENTS", $Events, "Id")
$sequencer.Allocate("EVENTS_PERFORMANCE", $Events, "EventsPerformance/Id")

#set($eTypes = $Unsafe.EventTypes)

$logger.Printf("eTypes: %v", $eTypes)

#set($validationResult = $http.Do("POST", "http://localhost:8871/dev/validate/event-perf", $eTypes))
#if($validationResult.Invalid)
$logger.Fatal("%v", $validationResult.Message)
#end

INSERT INTO EVENTS (
    ID,
    QUANTITY
) VALUES (
    $Events.Id,
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