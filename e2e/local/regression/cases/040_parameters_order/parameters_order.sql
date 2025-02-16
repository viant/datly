/* {"URI": "comprehensive/parameters-order", "Method": "POST"} */


import (
	"regression/cases/040_parameters_order.Events"
	"regression/cases/040_parameters_order.Data"
)


#set($_ = $Data<*Events>(body/Data).Output())
#set($_ = $Status<?>(output/status).Embed())

#set($_ = $Events<*Events>(body/Data))
#set($_ = $Jwt<string>(Header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Authorization  /*
   !!403

    SELECT Authorized /* {"DataType":"bool"} */
    FROM (
        SELECT (CASE WHEN $Jwt.UserID != 1 THEN 0 ELSE 1 END) AS Authorized
    ) t
    WHERE Authorized

*/)


$sequencer.Allocate("EVENTS", $Events, "Id")
#if($Unsafe.Events)
  $sql.Insert($Events, "EVENTS");
#end