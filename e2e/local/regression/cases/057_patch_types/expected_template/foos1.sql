/* {"URI":"basic/foos-many-many-custom","Method":"PATCH","ResponseBody":{"From":"Foos"}} */

import (
	"/Users/klarysz/Documents/datly/e2e/local/regression/cases/057_patch_types.Foos"
	"/Users/klarysz/Documents/datly/e2e/local/regression/cases/057_patch_types.Validation"
)


#set($_ = $Foos<[]*Foos>(body/))
#set($_ = $prevFoos<[]*Foos> /* {"Required":false}
  #set($FoosId = $Foos.QueryFirst("SELECT ARRAY_AGG(Id) AS Values FROM  `/`"))
  SELECT * FROM FOOS/* {"Selector":{}} */
  WHERE  #if($FoosId.Values.Length() > 0 ) ID IN ( $FoosId.Values ) #else 1 = 0 #end */
)


#set($_ = $prevFoosPerformance /* {"Required":false}
  #set($FoosId = $Foos.QueryFirst("SELECT ARRAY_AGG(Id) AS Values FROM  `/FoosPerformance/`"))
  SELECT * FROM FOOS_PERFORMANCE/* {"Selector":{}} */
  WHERE   #if($FoosId.Values.Length() > 0 ) ID IN ( $FoosId.Values ) #else 1 = 0 #end */
)

#set($Validation = $New("*Validation"))

#foreach($recFoos in $Unsafe.Foos)
   #set($preFoo = $prevFoosById[$recFoos.Id])
   $recFoos.Init($preFoo, $Validation)
   $recFoos. Validate($Validation)
#end

#if($Validation.IsValid == false)
    $response.Put("Message", $Validation.String())
    $response.StatusCode(400)
    $response.Failf("")
#end

$sequencer.Allocate("FOOS", $Foos, "Id")
$sequencer.Allocate("FOOS_PERFORMANCE", $Foos, "FoosPerformance/Id")
#set($prevFoosById = $prevFoos.IndexBy("Id"))
#set($prevFoosPerformanceById = $prevFoosPerformance.IndexBy("Id"))
#foreach($recFoos in $Unsafe.Foos)
#if($recFoos)
    #if(($prevFoosById.HasKey($recFoos.Id) == true))
      $sql.Update($recFoos, "FOOS");
    #else
      $sql.Insert($recFoos, "FOOS");
    #end
      #foreach($recFoosPerformance in $recFoos.FoosPerformance)
      #if($recFoosPerformance)
          #if(($prevFoosPerformanceById.HasKey($recFoosPerformance.Id) == true))
            #if(($recFoos.Id == $recFoosPerformance.FooId))
            $sql.Update($recFoosPerformance, "FOOS_PERFORMANCE");
            #end
          #else
            $sql.Insert($recFoosPerformance, "FOOS_PERFORMANCE");
          #end
      #end
      #end
#end
#end