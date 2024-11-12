/*
 {
  "URI": "vars/",
  "ConstURL": "./properties.json"
}
 */

#set( $_ = $Data<?>(output/view).Embed())


SELECT main.*
FROM (
         SELECT
             '$Var1 - $Vendor' AS Key1,
             '$Var2 - $Product' AS Key2,
             $Var3 as Key3 /* { "DataType": "bool" } */
         FROM $DummyTable
     ) main
