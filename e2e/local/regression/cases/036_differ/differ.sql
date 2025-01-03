/* {"URI":"basic/foos-differ","Method":"PUT","ResponseBody":{"From":"Foos"}} */

import (
	"regression/cases/036_differ.Foos"
)


#set($_ = $Foos<[]*Foos>(body/))
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true"'))

#set($_ = $FooIds<?>(param/Foos) /*
    ? SELECT ARRAY_AGG(Id) AS Values FROM  `/` LIMIT 1
*/)

    #set($_ = $Foos<*Foos>(body/))
	#set($_ = $CurFoosId<?>(param/Foos) /*
    ? SELECT ARRAY_AGG(Id) AS Values FROM  `/` LIMIT 1
    */)


    #set($_ = $FoosDBRecords /*
    ? SELECT * FROM FOOS
    WHERE $criteria.In("ID", $CurFoosId.Values)
    */)


#set($FoosDBRecordsIndex = $FoosDBRecords.IndexBy("Id"))
#foreach($recFoos in $Unsafe.Foos)
  $sql.Update($recFoos, "FOOS");

  #if($FoosDBRecordsIndex.HasKey($recFoos.Id) == false)
    $logger.Fatal("not found record with %v id", $recFoos.Id)
  #end

  #set($prevRec = $FoosDBRecordsIndex[$recFoos.Id])
  #set($fooDif = $differ.Diff($prevRec, $recFoos))

  #if($fooDif.Changed())
    INSERT INTO DIFF_JN(DIFF) VALUES ($fooDif.String());
  #end
#end