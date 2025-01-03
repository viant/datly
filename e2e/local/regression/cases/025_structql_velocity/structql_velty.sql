/*
{
   "URI":"basic/events-velty-validator",
   "Method":"POST",
   "ResponseBody": {"From": "Events"}
} */

import(
    "regression/cases/025_structql_velocity.Events"
)

#set($_ = $Events<*Events>(body/))
#set($_ = $Events<*Events>(body/).Output().Tag('anonymous:"true"'))

$sequencer.Allocate("EVENTS", $Events, "Id")
$sequencer.Allocate("EVENTS_PERFORMANCE", $Events, "EventsPerformance/Id")

#set($eTypes = $Events.Query("SELECT Price, Timestamp FROM `/EventsPerformance`"))

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