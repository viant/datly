/* {"URI":"user-metadata"} */

#set( $_ = $Fields<[]string>(query/fields).Optional().QuerySelector('user_metadata'))
#set( $_ = $Page<int>(query/page).Optional().QuerySelector('user_metadata'))

#set( $_ = $UserMetadata<?>(output/view).WithTag('anonymous:"true"'))


SELECT user_metadata.*
FROM (SELECT * FROM USER_METADATA t ) user_metadata

