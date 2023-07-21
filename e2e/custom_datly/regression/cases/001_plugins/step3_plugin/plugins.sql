/* {"URI":"basic/foos","Method":"POST","ResponseBody":{"From":"Foos"}} */

import (
	"foos.Foos"
)


#set($_ = $Foos<*Foos>(body/))

$Foos.Validate()
$sequencer.Allocate("FOOS", $Foos, "Id")


INSERT INTO FOOS(
ID,
NAME,
QUANTITY
) VALUES (
$Foos.Id,
$Foos.Name,
$Foos.Quantity
);
