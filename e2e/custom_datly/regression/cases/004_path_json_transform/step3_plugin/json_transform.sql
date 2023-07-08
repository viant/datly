/* {"URI":"basic/custom-json-path","Method":"POST","ResponseBody":{"From":"Bars"}} */

import (
	"bars_transform.Bars"
	"bars_transform.IntsTransformer"
)


#set($_ = $Bars<*Bars>(body/))
#set($_ = $Bars.Ints /* { "Transformer": "IntsTransformer" } */)
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