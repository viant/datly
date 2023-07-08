/* {"URI":"basic/foos-performance","Method":"POST","ResponseBody":{"From":"FoosPerformance"}} */

import (
	"foosPerformance.FoosPerformance"
)


#set($_ = $FoosPerformance<*FoosPerformance>(body/))
$sequencer.Allocate("FOOS_PERFORMANCE", $FoosPerformance, "Id")
$Unsafe.FoosPerformance.Validate()
INSERT INTO FOOS_PERFORMANCE( 
ID, 
PERF_NAME, 
PERF_QUANTITY, 
FOO_ID
) VALUES (
$FoosPerformance.Id, 
$FoosPerformance.PerfName, 
$FoosPerformance.PerfQuantity, 
$FoosPerformance.FooId
);
