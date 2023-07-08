/* {"URI":"basic/custom-json","Method":"POST","ResponseBody":{"From":"Bars"}} */

import (
	"bars.Bars"
)


#set($_ = $Bars<*Bars>(body/))
$sequencer.Allocate("BARS", $Bars, "Id")

#if($Bars)
    INSERT INTO BARS(
    ID,
    INTS,
    NAME
    ) VALUES (
    $Bars.Id,
    $Bars.Ints,
    $Bars.Name
    );
#endif