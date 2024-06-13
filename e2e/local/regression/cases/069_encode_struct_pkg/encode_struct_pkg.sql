/* {"URI":"vendors-check-pkg"} */

import (
   "regression/cases/069_encode_struct_pkg.VendorIds"
)

#set($_ = $IDs<[]string>(query/ids).WithCodec("Encode", "*encode_struct_pkg.VendorIds", "/" , "ID", "AccountID", "UserCreated").WithPredicate(0, "multi_in", "t"))
SELECT vendor.*
FROM (
        SELECT * FROM VENDOR t
             WHERE 1=1 ${predicate.Builder().CombineAnd(
                  $predicate.FilterGroup(0, "AND")
                ).Build("AND")}
    ) vendor
