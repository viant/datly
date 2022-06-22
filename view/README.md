### Resource

Resource groups and represents set of Views, Connectors, Parameters and Types needed to build all views provided in
resorce. In resource following sections can be defined:

| Section    | Description                                                            | Type                                 | Required |
|------------|------------------------------------------------------------------------|--------------------------------------|----------|
| SourceURL  |                                                                        | string                               | false    |
| Metrics    | View specific Metrics configuration inheritable by other Views Metrics | [Metrics](./README.md#Metrics)       | false    |
| Connectors | Database connector definitions inheritable by other Views Connectors   | [[]Connector](./README.md#Connector) | false    |
| Views      | Views definitions provided by the resource                             | [[]View](./README.md#View)           | false    |
| Parameters | Parameters configurations inheritable by other Views Parameters        | [[]Parameter](./README.md#Parameter) | false    |
| Types      | Inlined types definitions                                              | [[]Type](./README.md#Type)           | false    |
| Loggers    | Loggers inheritable by other Views Loggers                             | [[]Logger](./README.md#Logger)       | false    |

### View

For View description read one of the following docs: [programmatically usage](../go-usage.md)
or [yaml usage](../yaml-usage.md)

| Section              | Description                                                                       | Type                                         | Required                                    | Default              |
|----------------------|-----------------------------------------------------------------------------------|----------------------------------------------|---------------------------------------------|----------------------|
| Ref                  | Other View name that given View should inherit from                               | string                                       | false                                       |                      |
| Connector            | Connector used by the View                                                        | [Connector](./README.md#Connector)           | true                                        |                      |
| Name                 | Unique view name                                                                  | string, unique across the Resource           | true                                        |                      |
| Alias                | View table alias                                                                  | string                                       | false                                       |                      |
| Table                | Table name.                                                                       | string                                       | Table, FromURL or From need to be specified |                      |
| From                 | SQL inner select statement that will be used as source of data                    | string                                       | Table, FromURL or From need to be specified |                      |
| FromURL              | Source of the SQL in case when SQL is specified in different file                 | string                                       | Table, FromURL or From need to be specified |                      |
| Exclude              | Columns that should not be exposed when column detection is performed             | []string                                     | false                                       |                      |
| Columns              | Explicitly specified columns that current View can use                            | [[]Column](./README.md#Column)               | true                                        |                      |
| InheritSchemaColumns | Indicates whether all Columns that not match Struct Type should be removed or not | bool                                         | false                                       | false                |
| CaseFormat           | Database columns case format                                                      | [CaseFormat](./README.md#CaseFormat)         | false                                       | lowerunderscore      |
| Criteria             | Dynamic criteria expanded with the parameters values                              | string                                       | false                                       |                      |
| Selector             | Selector configuration                                                            | [SelectorConfig](./README.md#SelectorConfig) | false                                       |                      |
| Template             | Template configuration using parameters and velty syntax                          | [Template](./README.md#Template)             | false                                       |                      |
| Schema               | View schema type                                                                  | [Schema](./README.md#Schema)                 | false                                       |                      |
| With                 | View relations in order to produce results with nested objects                    | [[]Relation](./README.md#Relation)           | false                                       |                      |
| MatchStrategy        | Match strategy specific for given View                                            | [MatchStrategy](./README.md#MatchStrategy)   | false                                       | read_matched         |
| Batch                | Batch configuration specific for given View                                       | [Batch](./README.md#Batch)                   | false                                       | Batch{Parent: 10000} |
| Logger               | Logger specific for given View                                                    | [Logger](./README.md#Logger)                 | false                                       |                      |
| Counter              | Metrics specific for given View                                                   | [Metrics](./README.md#Metrics)               | false                                       |                      |

### Column

| Section    | Description                                                                                                                                                           | Type                                                              | Required | Default |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|----------|---------|
| Name       | Database column name                                                                                                                                                  | string                                                            | true     |         |
| DataType   | Column type                                                                                                                                                           | enum: `Int`, `Float`, `Float64`, `Bool`, `String`, `Date`, `Time` | false    |         |
| Expression | SQL expression i.e. COALESCE(price, 0)                                                                                                                                | string                                                            | false    |         |
| Filterable | Indicates whether column can be used using Selector criteria or not                                                                                                   | bool                                                              | false    | false   |
| Nullable   | Indicates whether column is nullable or not. In case if table is specified and DataType is not Time nor Date, Datly will automaticaly add COALESCE for the expression | bool                                                              | false    | false   |
| Default    | Default output value that will be replaced when zero value occur                                                                                                      | string                                                            | false    |         |
| Format     | Date output format                                                                                                                                                    | string                                                            | false    |         |

### SelectorConfig

SelectorConfig holds information about what can be used on specific View using Selectors built from the Request data.

| Section     | Description                                            | Type                                   | Required | Default                      |
|-------------|--------------------------------------------------------|----------------------------------------|----------|------------------------------|
| OrderBy     | Default Column that will be used to Sorted             | string                                 | false    |                              |
| Limit       | Maximum and default limit that can be used on the View | int                                    | false    | No default and maximum limit |
| Constraints | Selector constraints                                   | [Constraints](./README.md#Constraints) | false    | everything disabled          |

### Constraints

By default everything is forbidden. In order to allow datly create and populate Selectors from Http requests, it need to
be explicitly enabled.

| Section    | Description                                                                    | Type     | Required | Default |
|------------|--------------------------------------------------------------------------------|----------|----------|---------|
| Criteria   | Allows to parse _criteria into SQL statement                                   | boolean  | false    | false   |
| OrderBy    | Allows to parse _orderBy into SQL `order by`                                   | boolean  | false    | false   |
| Limit      | Allows to parse _limit into SQL `limit`                                        | boolean  | false    | false   |
| Offset     | Allows to parse _orrset into SQL `offset`                                      | boolean  | false    | false   |
| Filterable | Allowed columns to be used in the criteria, `*` in case of allowed all columns | []string | false    |         |

### Parameter

Parameters are defined in order to read data specific for the given http request.

| Section      | Description                                                    | Type                             | Required | Default      |
|--------------|----------------------------------------------------------------|----------------------------------|----------|--------------|
| Ref          | Other Parameter name that given Parameter will inherit from    | string                           | false    |              |
| Name         | Identifier used to access parameter value in the templates     | string                           | true     |              |
| PresenceName | Identifier used to check if parameter was set in the templates | string                           | false    | same as Name |
| In           | Source of the parameter                                        | [Location](./README.md#Location) | true     |              |
| Required     | Indicates if parameter is required or not                      | boolean                          | false    | false        |
| Description  | Parameter description                                          | string                           | false    |              |
| Schema       | Schema configuration                                           | [Schema](./README.md#Schema)     | true     |              |
| Codec        | Codec configuration                                            | [Codec](./README.md#Codec)       | false    |              |

### Location

| Section | Description                                                     | Type                                                         | Required |
|---------|-----------------------------------------------------------------|--------------------------------------------------------------|----------|
| Kind    | Represents the source of the parameter i.e. Header, QueryParam. | enum: `query`,`header`, `cookie`, `data_view`, `body`, `env` | true     |
| Name    | Parameter source identifier i.e. Authorization, userId          | string                                                       | true     |

### Codec

In some cases it is needed to transform raw parameter value to some different value. For example Authorization Header
with JWT Token. In this case it is needed to provide and configure Codec that will transform raw JWT token into some
struct. The Codec need to be created programmatically and provided during the Resource initialization.

The interface needed to be implemented by the Codec:

```go
Value(ctx context.Context, raw string) (interface{}, error)
```

| Section | Description                                                        | Type   | Required |
|---------|--------------------------------------------------------------------|--------|----------|
| Name    | Codec name, have to match the codec name provided programmatically | string | true     |

### Schema

Schema holds and defines actual type of the parent. It can either load type from the types provided programmatically, or
by generating type using predefined primitive types.

| Section  | Description                                                            | Type                                                              | Required                       |
|----------|------------------------------------------------------------------------|-------------------------------------------------------------------|--------------------------------|
| Name     | Schema name, needs to match one of the types provided programmatically | string                                                            | true unless DataType specified |
| DataType | Primitive data type name                                               | enum: `Int`, `Float`, `Float64`, `Bool`, `String`, `Date`, `Time` | true unless Name specified     |

### Type

In some cases the Type definition can be provided in the yaml file, it allows to use them in templates but they will not
be accessible programmatically. The usage for them might be f.e. parsing RequestBody to some struct.

| Section | Description                  | Type                         | Required |
|---------|------------------------------|------------------------------|----------|
| Name    | Type name                    | string                       | true     |
| Fields  | Metadata fields descriptions | [[]Field](./README.md#Field) | true     |

### Field

| Section | Description                                                                                                                                  | Type                         | Required                                                   | Default |
|---------|----------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|------------------------------------------------------------|---------|
| Name    | Struct field name                                                                                                                            | string, UpperCamelCase       | true                                                       |         |
| Embed   | Indicates whether field should be Anonymous (i.e. while parsing JSON, if field is Anonymous and type of Struct the Struct will be flattened) | bool                         | false                                                      | false   |
| Column  | Database column name                                                                                                                         | string                       | true unless name doesn't match actual database column name |         |
| Schema  | Field schema                                                                                                                                 | [Schema](./README.md#Schema) | Schema or Fields need to be specified                      |         |
| Fields  | Describes non-primitive field type                                                                                                           | [[]Field](./README.md#Field) | Schema or Fields need to be specified                      | ---     |

### Template

In order to create more complex and optimized SQL statements, the [velty](https://github.com/viant/velty)  syntax can be
used to produce SQL dynamically based on the Parameters created based on f.e. http request.

Namespace:

* `Has` - the parameter presence can be checked using the prefix `$Has`.
* `Unsafe` - to access raw parameter value in the template, you can use prefix `$Unsafe`. This namespace should be used
  extremely careful. It should be used only for the parameters `data_view` Kind, or only to check value using velty
  statements. The parameters without `$Unsafe` prefix will be replaced with placeholders.
* `View` - to access basic details about the current View in template, you can use `$View` prefix. Those values will be
  expanded as is. They will not be pushed as placeholders so it is important to wrap them with quotes.

| Section        | Description                                                                                                                                  | Type                                 | Required                                                   | Default |
|----------------|----------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------|------------------------------------------------------------|---------|
| Source         | The actual SQL template                                                                                                                      | string                               | true                                                       |         |
| SourceURL      | Indicates whether field should be Anonymous (i.e. while parsing JSON, if field is Anonymous and type of Struct the Struct will be flattened) | string                               | false                                                      | false   |
| Schema         | The actual type holder of the combined parameters. Each parameter name and type has to match the current Schema Type                         | [Schema](./README.md#Schema)         | false                                                      | false   |
| PresenceSchema | Similar to the Schema, but each Parameter name has to match field in the struct and every non primitive type has to be a boolean             | [Schema](./README.md#Schema)         | false                                                      | false   |
| Parameters     | Parameters that can be used inside the template                                                                                              | [[]Parameter](./README.md#Parameter) | false                                                      | false   |

### Connector

In order to communicate with database, database credentials need to be specified. To provide secure credentials store,
the DSN should be represented using variables (i.e. `${user}:${password}`)
and expanded using external store.

| Section | Description                                                    | Type                         | Required |
|---------|----------------------------------------------------------------|------------------------------|----------|
| Name    | Connector name                                                 | string                       | true     |
| Ref     | Other connector name which given connector should inherit from | string                       | false    |
| Driver  | Database driver                                                | string                       | true     |
| DSN     | Database source name, the uri needed to connect to database.   | string                       | true     |
| Secret  |                                                                | [Secret](./README.md#Secret) | true     |

### Secret

### Metrics

Used to collect data about View usage, including average time, success/failure ratio etc.

| Section | Description | Type   | Required |
|---------|-------------|--------|----------|
| URIPart |             | string | true     |

### Logger

Programmatically created and provided logger.

| Section | Description                                              | Type   | Required |
|---------|----------------------------------------------------------|--------|----------|
| Ref     | Other Logger name which given Logger should inherit from | string | false    |
| Name    | Logger name                                              | string | true     |

### Batch

Batches data fetched from database.

| Section | Description                                                                                           | Type | Required |
|---------|-------------------------------------------------------------------------------------------------------|------|----------|
| Parent  | Number of parent placeholders in `column in (?,?,?,?)` statement if View is a child of any other View | int  | false    |

### CaseFormat

Enum, possible values:

* `uu`, `upperunderscore` - i.e. EMPLOYEE_ID
* `lu`, `lowerunderscore` - i.e. employee_id
* `uc`, `uppercamel` - i.e. EmployeeId
* `lc`, `lowercamel` - i.e. employeeId
* `l`, `lower` - i.e. employeeid
* `u`, `upper` - i.e. EMPLOYEEID

### MatchStrategy

Enum, possible values:

* `read_matched`
* `read_all`

### Parameters Codecs

Supported built in datly codecs:

* `VeltyCriteria` - parses template using velocity syntax to sanitize criteria built from templates. Supported prefixes:
    * `Unsafe` - in order to access parameter values in the template, it is needed to use `Unsafe` prefix.
    * `Safe_Column` - if column with given parameter value doesn't exist the error will be thrown.
      i.e. `$Safe_Column.Columns[$i].Name`
    * `Safe_Value` - the parameter values with this prefix will be passed as binding parameters.
* `Strings` - splits string using `,` into `[]string`