# Datly - codeless rule based data layer service

[![GoReportCard](https://goreportcard.com/badge/github.com/viant/datly)](https://goreportcard.com/report/github.com/viant/datly)
[![GoDoc](https://godoc.org/github.com/viant/datly?status.svg)](https://godoc.org/github.com/viant/datly)


This library is compatible with Go 1.11+

Please refer to [`CHANGELOG.md`](../CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#motivation)
- [Usage](#usage)
- [License](#license)


## Motivation

The goal of this project is to simplify and speed up data layer prototyping and development.
This is achieved by utilising rules to govern data mapping and binding for all data interaction.
This project can be deployed as standalone or serverless with REST or Micro service style.

## Usage
One of the most important concept used across the datly, are `Views`. `View `represents database table, and also allows
filtering rows and columns. You can load them from `.yaml` files, as one of the Resources as follows:

```go
resource, err := NewResourceFromURL(context.TODO(), "<path to resource file>.yaml", Types{})
if err != nil {
//...handle error
}
view, err := resource.View("foos")
```

You can pass `Types` if you want to set type that will represent Database table. Don't forget to add `sqlx` if needed.

It is also possible to create `View` programmatically. In this case you can pass type by creating `Schema` :

```go
view := &View{
    Name: "foos", 
    Connector: &config.Connector{
            Name: "mydb",
            Driver: "sqlite", 
            DSN: "./mydb.db",
        },
    Schema: NewSchema(reflect.TypeOf(Foo{})),
}
```
This is the simplest `View` that allows to fetch data from `Database`. `Name` has to be unique across the application if `View` would be configured to handle HTTP Requests.
Not implemented yet (see [TODO](../TODO.md)). If `Type` was omitted, it will be built by using table columns type. It will be serializable, but it is not possible to use type assertion.

Note: If `View` was loaded from an external file, it would be also initialized and ready to use, otherwise, due to the optimization reasons, you should call `View.Init` before app starts.
After `View` initialization - it should be treated as immutable, since it should be created only once and shared where is needed.

In case if you don't want to publish all columns from database, you can either specify all publishable columns as follows:
```yaml
Views:
 - Name: view_name
   From: foos
 - Columns:
   - Name: id
     DataType: Int
   - Name: quantity
     DataType: Float
   - Name: event_type_id
     DataType: Int
```

In this case you have to also specify Column `DataType`. Or you can exclude columns that shouldn't be publishable:
```yaml
Views:
 - Name: view_name
   Table: foos
   Exclude:
   - id
   - event_type_id
```

Examples above also shows one thing. In the case if neither `Table` nor `From` was specified, the `View name` will be used as database source. It can either be a table name like `foos` or it can be `select statement`.

We can also specify expressions for each Column, i.e. to fetch value uppercased:
```yaml
Views:
  - Columns:
      - Name: name
        Expression: Upper(name)
        DataType: String
```

We can also modify sql select statement by providing default values with `Selector` and `Criteria#Expression`:
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
```

We can use `Client Selector` and specify what can be used using `SelectorConstraints`:
```yaml
Views:
  SelectorConstraints:
    Criteria: true
    OrderBy: true
    Limit: true
    Columns: true
    Offset: true
```

We can use relations to assemble more complex structs - like `Event` and `EventType`:

```yaml
Views:
  - Connector:
      Ref: mydb
    Name: events
    Table: events
    SelectorConstraints:
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

In example above we can see that we can use other `View` to initialize another using `Ref` property. Same goes to the `Connectors` and `Parameters`.
However referenced View Columns won't be inherited if View has any Relation (`With`).

We can use parameters to narrow results and add some security:
```yaml
Views:
  - Name: users
    Table: users
    Criteria:
      Expression: id = ${user_id}
    Parameters:
    - Ref: user_id

Parameters:
  - Name: user_id
    In:
      Kind: cookie
      Name: user-id
    Required: true
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

- Connector:
    Ref: otherdb
  Name: languages
  Table: languages
  BatchReadSize: 4
  Selector:
    Limit: 14
```

Default `MatchStrategy` is `read_matched` - referenced View will wait until Parent fetches result and will narrow fetched result
to only needed records (by `Column in (?,?,?)`). The other supported is `read_all` - the all specified parent values and all referenced view values will be fetched at the same time
then filtered and merged at the backend. We can also specify `BatchReadSize` - in example above, datly will fetch no more than 14 languages
in chunks no bigger than 4.

## Contributing to datly

Datly is an open source project and contributors are welcome!

See [TODO](./TODO.md) list

## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

<a name="Credits-and-Acknowledgements"></a>

## Credits and Acknowledgements

**Library Authors:** 
- Kamil Larysz
- Adrian Witas

