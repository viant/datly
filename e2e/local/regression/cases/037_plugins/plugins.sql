/* {"URI":"basic/foos-plugin","Method":"POST","ResponseBody":{"From":"Foos"}} */

import (
    "regression/cases/037_plugins.FooPlugin" AS "FooPlugin"
)

#set($_ = $Foos<[]*FooPlugin>(body/))
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true"'))

$sequencer.Allocate("FOOS", $Foos, "Id")
#foreach($recFoos in $Unsafe.Foos)
  $recFoos.Validate()
  $sql.Insert($recFoos, "FOOS");
#end