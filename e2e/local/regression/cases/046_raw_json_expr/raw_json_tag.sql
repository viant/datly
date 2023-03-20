/* {"URI":"raw_json_tag"} */


SELECT main.*
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* {"DataType": "json.RawMessage", "Tag":"jsonx:\",inline\""} */,
             CLASS_NAME as ClassName
         FROM OBJECTS
     ) main