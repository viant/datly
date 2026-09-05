/* {"URI":"/v1/api/dev/basic/events-many","Method":"POST","Connector":"dev"} */


import (
	"generate_post_basic_many.Events"
	)


#set($_ = $Events<[]Events>(body/).WithTag('anonymous:"true"').Required())
	#set($_ = $CurEventsId<?>(param/Events) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/` LIMIT 1
*/
)
	#set($_ = $CurEvents<[]*Events>(view/CurEvents) /*
? SELECT * FROM EVENTS
WHERE $criteria.In("ID", $CurEventsId.Values)
*/
)
#set($_ = $Events<[]>(body/).WithTag('anonymous:"true"  typeName:"Events"').Required().Output())



$sequencer.Allocate("EVENTS", $Events, "Id")


#foreach($RecEvents in $Events)
$sql.Insert($RecEvents, "EVENTS");
#end