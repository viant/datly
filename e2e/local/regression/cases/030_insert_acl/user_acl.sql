/*
{
   "URI":"basic/user_acl",
   "Method":"POST",
   "RequestBody":{
        "DataType": "Events"
   },
    "ResponseBody": {"From": "Events"}
} */


import(
    "regression/cases/030_insert_acl.Events"
)

#set($_ = $Jwt)
#set($_ = $Events<*Events>(body/))

#set($Acl = $Unsafe.UserAcl /*
  { "Auth":"Jwt", "Connector":"dyndb" }
                          SELECT USER_ID AS UserID,
                          ARRAY_EXISTS(ROLE, 'READ_ONLY') AS IsReadOnly,
                          ARRAY_EXISTS(FEATURE1, 'FEATURE1') AS Feature1
                          FROM $DB["dyndb"].USER_ACL WHERE USER_ID = $Jwt.UserID
 */)

$sequencer.Allocate("EVENTS", $Events, "Id")
$sequencer.Allocate("EVENTS_PERFORMANCE", $Events, "EventsPerformance/Id")

#if($Acl.IsReadOnly)
$logger.Fatal("permission denied for %v", $Jwt.Email)
#end


INSERT INTO EVENTS (
    ID,
    QUANTITY
) VALUES (
    $Events.Id /* {"DataType":"Events","Target":"","Cardinality":"One"} */ ,
    $Events.Quantity
);
