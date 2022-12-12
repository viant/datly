/*
{
   "URI":"basic/user_acl",
   "Method":"POST",
   "Declare":{
    "Events":"*regression/cases/030_insert_acl.Events"
    },
   "RequestBody":{
        "DataType": "Events"
   },
    "ResponseBody": {"From": "Events"}
} */




#set($acl=$Unsafe.UserAcl /*
  {"Auth":"Jwt", "Connector":"dyndb"}   SELECT USER_ID AS UserID,
                          ARRAY_EXISTS(ROLE, 'READ_ONLY') AS IsReadOnly,
                          ARRAY_EXISTS(FEATURE1, 'FEATURE1') AS Feature
                    FROM USER_ACL WHERE USER_ID = $Jwt.UserID
 */)


$sequencer.Allocate("EVENTS", $Events, "Id")
$sequencer.Allocate("EVENTS_PERFORMANCE", $Events, "EventsPerformance/Id")

#if($acl.IsReadOnly)
$logger.Fatal("permission denied for %v", $Jwt.Name)
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