/* {"URI":"/deactivate", "Method":"GET"} */

#set($_ = $Module<string>(query/module))
#set($_ = $Views<[]string>(query/view))

#if(($Unsafe.Module=="") && ($Unsafe.Views.Length() == 0))
    $logger.Fatal("both module and view were empty")
#end

UPDATE DATLY_JOBS SET Deactivated = 1  WHERE
#if($Unsafe.Views.Length() >  0)
    $criteria.In("ID", $Views)
#elseif($Has.Module==true)
  Module = $Module
#end
;
