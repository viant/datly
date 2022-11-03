### Supported Hints

The option package describes options that can be specified using Hints with given grammar:

```
[RouteConfig]
SELECT events.*     [OutputConfig] EXCEPT ID,
       eventTypes.* [OutputConfig]
       FROM (
            SELECT ev.ID            [ColumnConfig],
                   ev.NAME          [ColumnConfig],
                   ev.event_type_id [ColumnConfig]
            FROM events 
       ) events [ViewConfig]
       JOIN (
            SELECT typ.ID [ColumnConfig]                   
            FROM event_types
       ) eventTypes [ViewConfig] ON eventTypes.ID = events.event_type_id
```