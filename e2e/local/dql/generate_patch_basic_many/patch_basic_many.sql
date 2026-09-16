/* {"URI":"/v1/api/dev/basic/foos-many","Method":"PATCH","Connector":"dev"} */


import (
	"generate_patch_basic_many.Foos"
	)


#set($_ = $Foos<[]Foos>(body/).WithTag('anonymous:"true"').Required())
	#set($_ = $CurFoosId<?>(param/Foos) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/` LIMIT 1
*/
)
	#set($_ = $CurFoos<[]*Foos>(view/CurFoos) /*
? SELECT * FROM FOOS
WHERE $criteria.In("ID", $CurFoosId.Values)
*/
)
#set($_ = $Foos<[]>(body/).WithTag('anonymous:"true"  typeName:"Foos"').Required().Output())



$sequencer.Allocate("FOOS", $Foos, "Id")

#set($CurFoosById = $CurFoos.IndexBy("Id"))

#foreach($RecFoos in $Foos)
  #if($CurFoosById.HasKey($RecFoos.Id) == true)
$sql.Update($RecFoos, "FOOS");
  #else
$sql.Insert($RecFoos, "FOOS");
  #end
#end