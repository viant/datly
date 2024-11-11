/* {"URI":"raw_json_tag"} */

#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))


SELECT main.*
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* {"DataType": "json.RawMessage", "Tag":"jsonx:\",inline\""} */,
             CLASS_NAME as ClassName
         FROM OBJECTS
         WHERE ID != 999
     ) main