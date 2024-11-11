/* {"URI":"vendors-check"} */

import (
   "regression/cases/065_encode_struct.VendorIds"
)

#set($_ = $IDs<[]string>(query/ids).WithCodec("Encode", "*VendorIds", "/" , "ID", "AccountID", "UserCreated").WithPredicate(0, "multi_in", "t"))
#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))

SELECT vendor.*
FROM (
        SELECT * FROM VENDOR t
             WHERE 1=1 ${predicate.Builder().CombineAnd(
                  $predicate.FilterGroup(0, "AND")
                ).Build("AND")}
    ) vendor
