SELECT eventTypes.*,
       events.* /* {"Cardinality": "Many" } */
FROM (
     SELECT * FROM event_types
         ) eventTypes
JOIN (
    SELECT * FROM events /* {"ExecKind": "service" } */
    ) events ON events.event_type_id = eventTypes.id