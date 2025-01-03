/* {"URI":"composition/xxx",
    "Method":"GET",
    "Type":"handler.ProxyProvider",
    "HandlerArgs":["GET", "/v1/api/dev/vendors"]
} */

#set( $_ = $Fields<[]string>(query/fields).Value('id,name'))
