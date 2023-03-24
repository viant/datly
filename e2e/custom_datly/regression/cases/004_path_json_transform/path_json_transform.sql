/* {"URI":"basic/custom-json-path","Method":"POST","ResponseBody":{"From":"Bars"}} */

import (
	"rules/004_path_json_transform/dsql.Bar"
	"rules/004_path_json_transform/dsql.IntsTransformer"
)


#set($_ = $Bars<*Bar>(body/))
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