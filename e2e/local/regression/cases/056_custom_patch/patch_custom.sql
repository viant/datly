/* {"URI":"basic/patch-custom","Method":"PATCH","ResponseBody":{"From":"Foos"}} */

import (
	"regression/cases/056_custom_patch.Foos"
	"regression/cases/056_custom_patch.FoosPerformance"
)


#set($_ = $Foos<[]*Foos>(body/))
	#set($_ = $CurFoosId<?>(param/Foos) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/` LIMIT 1
*/
)
	#set($_ = $CurFoosFoosPerformanceId<?>(param/Foos) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/FoosPerformance` LIMIT 1
*/
)
	#set($_ = $CurFoosPerformance<[]*FoosPerformance>(data_view/CurFoosPerformance) /*
? SELECT * FROM FOOS_PERFORMANCE
WHERE $criteria.In("ID", $CurFoosFoosPerformanceId.Values)
*/
)
	#set($_ = $CurFoos<[]*Foos>(data_view/CurFoos) /*
? SELECT * FROM FOOS
WHERE $criteria.In("ID", $CurFoosId.Values)
*/
)


$sequencer.Allocate("FOOS", $Foos, "Id")

$sequencer.Allocate("FOOS_PERFORMANCE", $Foos, "FoosPerformance/Id")

#set($CurFoosById = $CurFoos.IndexBy("Id"))
#set($CurFoosPerformanceById = $CurFoosPerformance.IndexBy("Id"))

#foreach($RecFoos in $Foos)

  #if($CurFoosById.HasKey($RecFoos.Id) == true)
    INSERT INTO FOOS_CHANGES (PREVIOUS) VALUES ($json.Marshal($CurFoosById[$RecFoos.Id]));
  #end

    #if($CurFoosById.HasKey($RecFoos.Id) == true)
        $sql.Update($RecFoos, "FOOS");
      #else
        $sql.Insert($RecFoos, "FOOS");
    #end

    #foreach($RecFoosPerformance in $RecFoos.FoosPerformance)
        #set($RecFoosPerformance.FooId = $RecFoos.Id)
        #if($CurFoosPerformanceById.HasKey($RecFoosPerformance.Id) == true)
            $sql.Update($RecFoosPerformance, "FOOS_PERFORMANCE");
        #else
            $sql.Insert($RecFoosPerformance, "FOOS_PERFORMANCE");
        #end
    #end
#end