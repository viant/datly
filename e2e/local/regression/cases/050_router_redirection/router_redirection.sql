/* {"URI":"composition/xxx"} */

#set( $_ = $Http<?>(query/fields).Value('id,name,bar'))
#set( $_ = $Http<?>(http/).Redirect("/foo-redirected"))

