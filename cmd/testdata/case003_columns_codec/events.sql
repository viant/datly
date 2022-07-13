SELECT events.*,
       name,
       (CASE
            WHEN COLUMN_X = 1 THEN
                'x1,x2'
            WHEN COLUMN_X = 2 THEN
                'x3,x4'
            ELSE ''
           END) AS slice /* {"Codec":{"Ref":"AsStrings"}}  */
FROM events as events