/* {"URI":"basic/foos-inserter","Method":"POST","ResponseBody":{"From":"Foos"}} */

#set($_ = $Foos<[]*FooPlugin>(body/))
$sequencer.Allocate("FOOS", $Foos, "Id")
#foreach($recFoos in $Unsafe.Foos)
  $sql.Insert($recFoos, "FOOS");
#end