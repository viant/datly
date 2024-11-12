/* {"URI":"raw_json_expr"} */

#set( $_ = $Data<?>(output/view).Embed())


SELECT main.*,
       cast(main.Name AS string)
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* {"DataType": "json.RawMessage"} */,
             (OBJECT->'$.Name') AS Name /* {"DataType":"string"} */,
             CLASS_NAME as ClassName
         FROM OBJECTS
         WHERE ID != 999
     ) main