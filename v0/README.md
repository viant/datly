# Datly - codeless rule based data layer service

[![GoReportCard](https://goreportcard.com/badge/github.com/viant/datly)](https://goreportcard.com/report/github.com/viant/datly)
[![GoDoc](https://godoc.org/github.com/viant/datly?status.svg)](https://godoc.org/github.com/viant/datly)

This is original POC (deprected now see root project)

This library is compatible with Go 1.11+

Please refer to [`CHANGELOG.md`](../CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#motivation)
- [Usage](#usage)
- [Configuration Rule](#configuration-rule)
- [Request filters](#request-filters)
- [Data view hooks](#data-view-hooks)
- [End to end testing](#end-to-end-testing)
- [License](#license)


## Motivation

The goal of this project is to simplify and speed up data layer prototyping and development.
This is achieved by utilising rules to govern data mapping and binding for all data interaction.
This project can be deployed as standalone or serverless with REST or Micro service style.    

## Usage

Datly uses data connectors and rules to match http data request path.  

Connector simply configures driver, credentials and target dataset. 

For example:
- [MySQL Connector](usage/connectors/myslql_meta.yaml)
- [BigQuery Connector](usage/connectors/bigquery_dw.yaml)

A rule control a data tier  

#### Simple rule

The following rule, defines '/v1/api/evnet_types' path match and event_types table 

[evnet_types.yaml](v0/usage/rules/evnet_types.yaml)
```yaml
path: /v1/api/evnet_types
views:
  - table: evnet_types
    connector: myslql_meta
```

Now you can use simple get request to retrieve data 
```bash
## to get all event_types use simple get request
curl -H "Content-Type: application/yaml"  http://datlyEndpoint/v1/api/evnet_types

## to get all billable event_types, with specific columns, limit and offset, use get request
curl http://datlyEndpoint/v1/api/evnet_types?_criteria=billable:true&_fields=id,name,billable&_limit=10&_offset=20
```

#### Data View References

Datly support cross vendor/data view references with One and Many cardinality. All queries resulting in the output run concurrently.

[events.yaml](usage/rules/events.yaml)
```yaml
path: /v1/api/events
views:
  - table: events
    alias: e
    connector: bigquery_dw
    refs:
      - name: type
        cardinality: One
        dataView: event_types
        'on':
          - column: event_type_id
            refColumn: id

  - table: event_types
    connector: myslql_meta
    selector:
      prefix: tpy
```
Now you can use simple get request to retrieve data 
```bash
## to get all event_types use simple get request
curl -H "Content-Type: application/yaml"  http://datlyEndpoint/v1/api/events

## to get events with specific column from event and event_types tables you can use:
curl http://datlyEndpoint//v1/api/events?_fields=id,name,event_type_id,tpy_fields=id,name
```

#### Dynamic Data Binding

All rules can use $variables to dynamically access Path and QueryString parameters, body, headers, external table/view's data.

The following rule defines binding that is evaluated from external table.

[events.yaml](usage/rules/bindings.yaml)
```yaml
path: /v1/api/account/{accountID}/events
views:
  - table: events
    connector: bigquery_dw
    criteria:
      expression: event_type_id IN ($types)

    bindings:
      - placeholder: types
        dataView: event_types
        default: 0

  - table: event_types
    connector: myslql_meta
    selector:
      columns: [id]
    criteria:
      expression: account_id = $accountID
```

#### SQL Base Data View

Data view can use a table or SQL to define data access. SQL can be inlined in rule or deletated to external file.

[vevents.yaml](usage/rules/vevents.yaml)
```yaml
path: /v1/api/vevents
views:
  - name: vevents
    connector: db1
    from: 
      SQL: SELECT
        e.*,
        t.name AS event_type_name,
        t.account_id,
        a.name AS account_name
        FROM events e
        LEFT JOIN event_types t ON t.id = e.event_type_id
        LEFT JOIN accounts a ON a.id = t.account_id
        ORDER BY 1
```

#### View Templates

For REST style API, the following rules use view template to control/override only specific view aspect.

Rules:

- [/v1/api/events](usage/rules/uri/events.yaml)
- [/v1/api/events/{id}](usage/rules/uri/event.yaml)
- [/v1/api/events/{id}/type](usage/rules/uri/type.yaml)

Template:
- [events template](usage/tmpl/events.yaml)


#### Controlling output case format

You can control output case format with caseFormat settings.

[events.yaml](usage/rules/case_format.yaml)
```yaml
path: /v1/api/events
output:
  - dataView: events
    caseFormat: LowerCamel

views:
  - table: events
    caseFormat: LowerUnderscore
    connector: db1
```


#### Request filters

Request filter has the following signature
```go
type Filter func(ctx context.Context, request *Request, writer http.ResponseWriter) (toContinue bool, err error)
```

Request filters run data service is invoked, so you can add custom authentication, permission check logic.  

To register filter for reader service:

```go
    reader.Filters().Add(myFitler)
```

#### Data view hooks

Each data view can enrich result dataset with custom visitor code, the following rule uses
SetColor visitor function to modify data view output.


[events.yaml](usage/rules/onread.yaml)
```yaml
path: /v1/api/events
views:
  - table: events
    connector: db
    onRead:
      visitor: EventsColor
```


```go
func SetEventsColor(ctx *data.Context, object *data.Value) (b bool, err error) { {
    quantity, err := object.FloatValue("quantity")
    if err != nil || quantity == nil {
        return true, err
    }
    if *quantity > 10 {
        object.SetValue("color", "orange")
    } else {
        object.SetValue("color", "green")
    }
    return true, nil
}

data.VisitorRegistry().Register(useCase.visitor, useCase.visit)

```



## Configuration Rule

Configuration rule can use JSON or YAML format

#### Data View
Data view defines data source:

- Name: data view name
- Table: table
- Alias: table or SQL alias
- From: SQL
- FromURL: SQL URL
- Columns: table, view columns
- Criteria: fixed criteria that can not be control by selector
- CaseFormat: data view storage case format
- Refs data view references
- HideRefIDs: flag to hide reference id column 
- Cache: optional caching options

### Caching option

- Service: registered cache service name
- TTLMs: time to live

### Parameter pool
Parameter pool is a map assembled from query string parameters, request body, path parameters.
You can access any parameters pool with ${variable}.

### Binding

Binding allows to define/redefine parameter pool

- Name: source parameter name
- Placeholder: name in the parameter pool
- Type: one of the following
       * Path, path source where path uses {} to define parameters
       * QueryString
       * DataView
       * Header
- Default: default value

#### Selector

Selector is client side control for data projection (columns) and selection (criteria, limit, offset) and data order.  
Selector is assembled for each data view from parameters pool, data view can control selector prefix with
selector.prefix attribute.  

- Prefix: parameter pool matching pregix
- Columns: coma separated list of columns  
- Criteria: where clause, note that you can not run SQL injection when dataView criteria is used.  
- OrderBy: coma separated list of columns  
- Limit: output limit     
- Offset: output offset
 

#### Reference

Reference define association between an owner and reference data view.

- Name: output field name
- Cardinality: reference cardinality: One or Many
- DataView: reference data view
- On: owner and reference keys match array

#### Input/Output

Input/Output defines collection of data input and output rule
If left empty if uses firs view name as output.

- DataView: source data view name 
- Key: output data key
- CaseFormat: output case format
- Cardinality: cardinality of input or output


Use [e2e test](e2e/README.md#use-cases) case rule to see actual rule examples.

## Deployment

TODO add section here

## End to end testing

Datly is end to end tested, including stand-alone and serverless mode (AWS API Gateway)

You can try on all data ingestion by simply running e2e test cases:

- [Prerequisites](e2e/README.md#prerequisites)
- [Use cases](e2e/README.md#use-cases)


## Contributing to datly

Datly is an open source project and contributors are welcome!

See [TODO](../TODO.md) list

## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

<a name="Credits-and-Acknowledgements"></a>

## Credits and Acknowledgements

**Library Author:** Adrian Witas

