/* {"URI":"user-metadata"} */

#set( $_ = $Fields<[]string>(query/fields).Optional().QuerySelector('vendor'))
#set( $_ = $Page<int>(query/page).Optional().QuerySelector('vendor'))

#set( $_ = $UserMetadata<?>(output/view).WithTag('anonymous:"true"'))


SELECT user_metadata.*
FROM (SELECT * FROM USER_METADATA t ) user_metadata

