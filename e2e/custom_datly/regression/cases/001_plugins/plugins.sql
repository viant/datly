/* {"URI":"basic/foos","Method":"POST","ResponseBody":{"From":"Foos"}} */

import (
	"rules/001_plugins/dsql.Foos"
)


#set($_ = $Foos<*Foos>(body/))
$Unsafe.Foos.Validate()

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
