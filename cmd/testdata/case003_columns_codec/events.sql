SELECT events.*,
       event_type.*
FROM (
         SELECT id,
                event_type_id,
                (CASE
                     WHEN id = 1 THEN
                         'x1,x2'
                     ELSE
                         'x3,x4'
                    END) AS slice /* {"Codec":{"Ref":"AsStrings"}, "DataType": "string"}  */
         FROM events
     ) events
         JOIN event_types event_type ON events.event_type_id = event_type.id