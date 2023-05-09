/* {"URI":"basic/foos-many-many-custom","Method":"PATCH","ResponseBody":{"From":"Foos"}} */

import (
	"regression/cases/056_custom_patch.Foos"
)


#set($_ = $Foos<[]*Foos>(body/))
#set($_ = $prevFoos /* {"Qualifiers":[{"Column":"ID","Value":"Foos.Id"}]} 
  
  SELECT foos.*,
         foosPerformance.*
  FROM (SELECT * FROM FOOS) foos
  JOIN (SELECT * FROM FOOS_PERFORMANCE) foosPerformance on foos.ID = foosPerformance.FOO_ID
   */
)
$sequencer.Allocate("FOOS", $Foos, "Id")
$sequencer.Allocate("FOOS_PERFORMANCE", $Foos, "FoosPerformance/Id")
#set($prevFoosById = $prevFoos.IndexBy("Id"))
#set($prevFoosPerformanceById = $prevFoos.Query("SELECT * FROM `/FoosPerformance/`").IndexBy("Id"))
#foreach($recFoos in $Unsafe.Foos)
#if($recFoos)
    #if($prevFoosById.HasKey($recFoos.Id))
        INSERT INTO FOOS_CHANGES (PREVIOUS) VALUES ($json.Marshal($prevFoosById[$recFoos.Id]));
    #end

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