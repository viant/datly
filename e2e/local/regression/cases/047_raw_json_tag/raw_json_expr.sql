/* {"URI":"raw_json_expr"} */


SELECT main.*
FROM (
         SELECT
             ID as Id,
             OBJECT AS Preferences /* {"DataType": "json.RawMessage"} */,
             (OBJECT->'$.Name') AS Name /* {"DataType":"string"} */,
             CLASS_NAME as ClassName
         FROM OBJECTS
     ) main