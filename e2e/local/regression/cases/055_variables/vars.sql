/*
 {
  "URI": "vars/",
  "Const": {
    "Vendor": "VENDOR",
    "Product": "PRODUCT",
    "Var1": "setting1",
    "Var2": "setting2"
  }
}
 */
SELECT main.*
FROM (
    SELECT Key1, Key2 FROM (SELECT '$Var1 - $Vendor' AS Key1, '$Var2 - $Product' AS Key2) t
) main
