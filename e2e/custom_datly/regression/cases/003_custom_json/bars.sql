/* {"URI":"basic/custom-json","Method":"POST","ResponseBody":{"From":"Bars"}} */

import (
	"rules/003_custom_json/dsql.Bar"
)


#set($_ = $Bars<*Bar>(body/))
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