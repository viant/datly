/* {"URI":"basic/foos-differ","Method":"PUT","ResponseBody":{"From":"Foos"}} */

import (
	"regression/cases/036_differ.Foos"
)


#set($_ = $Foos<[]*Foos>(body/))
#set($_ = $FoosDBRecords /* {"Required":false}
  #set($FoosId = $Foos.QueryFirst("SELECT ARRAY_AGG(Id) AS Values FROM  `/`"))
  SELECT * FROM FOOS/* {"Selector":{}} */
  WHERE  #if($FoosId.Values.Length() > 0 ) ID IN ( $FoosId.Values ) #else 1 = 0 #end */
)

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