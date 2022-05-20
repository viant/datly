### Load Views from yaml

One of the most important concept used across the datly, are `Views`. `View `represents database table, and also allows
filtering rows and columns. You can also pass `Types`, in this case key has to match `Schema.Name`:

```go
resource, err := NewResourceFromURL(context.TODO(), "<path to resource file>.yaml", Types{})
if err != nil {
//...handle error
}

view, err := resource.View("foos")
if err != nil {
//...handle error
}
```

The simplest `Resource` read from file:
```yaml
Views:
  - Connector:
      Ref: mydb
    Name: events
Connectors:
  - Name: mydb
    Driver: sqlite3
    DSN: "./testdata/db/mydb.db"
```

If no `Schema` is provided, `View` type will be created using database table column types. It is handy, but it allows you only to serialize objects. Type assertion will not be able to use.
`View` name has to be unique. 

You can explicitly say which columns do you want to fetch by specifying `Columns`, you need to also specify each `Column#DataType` 
```yaml
Views:
  - Connector:
     Ref: mydb
   Name: view_name
   From: foos
   Columns:
   - Name: id
     DataType: Int
   - Name: quantity
     DataType: Float
     Expression: round(quantity, 0)
   - Name: event_type_id
     DataType: Int

Connectors:
  - Name: mydb
    Driver: sqlite3
    DSN: "./testdata/db/mydb.db"
```

You can also explicitly tell which columns you don't want to expose:
```yaml
Views:
  - Connector:
    Ref: mydb
    Name: view_name
    Table: foos
    Exclude:
     - id
     - event_type_id

Connectors:
  - Name: mydb
    Driver: sqlite3
    DSN: "./testdata/db/mydb.db"
```
Examples above also shows one thing. In the case if neither `Table` nor `From` was specified, the `View name` will be used as database source. It can either be a table name like `foos` or it can be `select statement`.

You can also modify sql select statement by providing default values with `Selector` and `Criteria#Expression`:
```yaml
Views:
  - Connector:
      Ref: mydb
    Name: events
    Table: events
    Columns:
      - Name: id
        DataType: Int
      - Name: timestamp
        DataType: String
      - Name: quantity
        DataType: Float
    Selector:
      OrderBy: id
      Limit: 100
    Criteria:
      Expression: quantity > 30
Connectors:
  - Name: mydb
    Driver: sqlite3
    DSN: "./testdata/db/mydb.db"
```

You can use `Client Selector` and define  `Selector.Constraints` to control what can be used by client:
```yaml
Views:
  - Connector:
      Ref: mydb
    Name: events
    Table: events
    Selector:
      Constraints:
          Criteria: true
          OrderBy: true
          Limit: true
          Columns: true
          Offset: true
Connectors:
  - Name: mydb
    Driver: sqlite3
    DSN: "./testdata/db/mydb.db"
```

You can use relations to assemble more complex structs - like `Event` and `EventType`:
```yaml
Views:
  - Connector:
      Ref: mydb
    Name: events
    Table: events
    Selector:
      Constraints:
          Columns: true

  - Connector:
      Ref: mydb
    Name: event_types

  - Connector:
      Ref: mydb
    Name: event_event-types
    Ref: events
    With:
      - Name: event_event-types
        Of:
          Ref: event_types
          Column: id

        Cardinality: One
        Column: event_type_id
        Holder: EventType
        IncludeColumn: false

Connectors:
  - Name: mydb
    Driver: sqlite3
    DSN: "./testdata/db/mydb.db"
```

View `event_event-types` can be used to assemble Event and EventType together. `With#Column` points to the table column `event_type_id` of the `event` table.
`Of#Column` points to the column `id` of the `event_type` table. Values of each of the columns must match each other in order to assemble `EventType` to the `Event` object.
The field that will hold the `EventType` will be created where name will be the same as `With#Holder`.
`With#Cardinality` indicates whether the `Holder` should be a single object (for cardinality `One`)
or slice of objects (for cardinality `Many`) . Both of the tables can be located in different databases. Connector will be used to obtain database connection.

Parameters allow changing sql statement depending on `*http.Request` or other `View`:
```yaml
Views:
  - Connector:
      Ref: mydb
    Name: users
    Table: users
    Criteria:
      Expression: id = ${user_id}
    Parameters:
    - Ref: user_id

Parameters:
  - Name: user_id
    In:
      Kind: path
      Name: user-id
    Required: true

Connectors:
- Name: mydb
  Driver: sqlite3
  DSN: "./testdata/db/mydb.db"
```
Parameters can be extracted from:
* cookie - `Kind: cookie`
* header - `Kind: header`
* query params - `Kind: query`
* path variable - `Kind: path`
* from database using another View - `Kind: data_view`. In this case - only one column has to be returned from database.

We can also specify `MatchStrategy` and `BatchReadSize` in order to optimize fetching data from database:
```yaml
Views:
- Connector:
    Ref: mydb
  Name: articles
  Table: articles
  MatchStrategy: read_all
  With: 
    - Name: articles_languages
      Of:
        Ref: languages
        Column: id

      Cardinality: One
      Column: lang_id
      Holder: Language

- Connector:
    Ref: otherdb
  Name: languages
  Table: languages
  BatchReadSize: 4
  Selector:
    Limit: 14

Connectors:
  - Name: mydb
    Driver: sqlite3
    DSN: "./testdata/db/mydb.db"

  - Name: otherdb
    Driver: sqlite3
    DSN: "./testdata/db/other.db"
```

Default `MatchStrategy` is `read_matched` - referenced View will wait until Parent fetches result and will narrow fetched result
to only needed records (by `Column in (?,?,?)`). The other supported is `read_all` - the all specified parent values and all referenced view values will be fetched at the same time
then filtered and merged at the backend. We can also specify `BatchReadSize` - in example above, datly will fetch no more than 14 languages
in chunks no bigger than 4.