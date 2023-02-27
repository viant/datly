### Resource

Router provides REST Api layer. Internally it uses the `Views` in order to communicate with database and add extra layer
to handle http requests in order to read and filter data. It can be configured programmatically or by reading external
configuration from `yaml` file.

In the `yaml` file, following sections can be configured:

| Section     | Description                                                                                      | Type                                   | Required |
|-------------|--------------------------------------------------------------------------------------------------|----------------------------------------|----------|
| Routes      | configuration for specific route                                                                 | [[]Route](./README.md#Route)           | true     |
| Resource    | configuration of Views, Connectors and Parameters shared across the Routes                       | [Resource](../view/README.md#Resource) | false    |
| Compression | Compression configuraion that will be used for all Routes unless Route Compression is configured | [Compression](./README.md#Compression) | false    |   
| Cors        | Cors configuraion that will be used for all Routes unless Route Cors is configured               | [Cors](./README.md#Cors)               | false    |  
| APIURI      |                                                                                                  | string                                 | true     |  
| SourceURL   |                                                                                                  | string                                 | false    |  
| With        |                                                                                                  | []string                               | false    |  

### Cors

In order to enable the web browser cross-origin requests, the Cors need to be configured. If any of the section is not
specified, corresponding Http header will not be added to the Cors preflight request:

| Section          | Description                                   | Type     | Required |
|------------------|-----------------------------------------------|----------|----------|
| AllowCredentials | Access-Control-Allow-Credentials header value | bool     | false    |
| AllowHeaders     | Access-Control-Allow-Headers header value     | []string | false    |
| AllowMethods     | Access-Control-Allow-Methods header value     | []string | false    |
| AllowOrigins     | Access-Control-Allow-Origin header value      | []string | false    |
| ExposeHeaders    | Access-Control-Expose-Headers header value    | []string | false    |
| MaxAge           | Access-Control-Max-Age header value           | int      | false    |

### Compression

In order to compress data if response exceed given size, Compression configuration need to be specified:

| Section   | Description                                                 | Type | Required | Default value |
|-----------|-------------------------------------------------------------|------|----------|---------------|
| MinSizeKb | Minimum size in KB after when response should be compressed | int  | false    | 0             |

### Route

Route configures specific URL handler for given http method. The Compression and Cors can be configured on the router
level, but can also be overridden on the `Route` level. In the Route section, following properties can be configured.

| Section          | Description                                                                                                                                                                                       | Type                                                                                     | Required | Default value             |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|----------|---------------------------|
| URI              | Url pattern that Requests will be matched with. The path variables have to be put between brackets i.e. `/users/{userId}`                                                                         | string                                                                                   | true     | ""                        |
| View             | View definition used to fetch data from database                                                                                                                                                  | [View](../view/README.md#View)                                                           | true     | null                      |
| Method           | Http method used to match http requests                                                                                                                                                           | enum:  `GET`, `POST`                                                                     | true     | ""                        |
| Service          | The service type used to handle the requests.                                                                                                                                                     | enum: `Reader`                                                                           | false    | GET -> Reader, Post -> "" |
| Cors             | Cors configuration specific for the Route                                                                                                                                                         | [Cors](./README.md#Cors)                                                                 | false    | null                      |
| Cardinality      | Indicates whether single object should be returned or the array of objects.                                                                                                                       | enum: `ONE`, `MANY`                                                                      | false    | MANY                      |
| CaseFormat       | Configures the output JSON field names format.                                                                                                                                                    | enum: [CaseFormat](../view/README.md#CaseFormat)                                         | false    | uppercamel                |
| OmitEmpty        | Removes zero values from the output. Examples of removable values: `0`, `""`, `false`, `[]`, `null`                                                                                               | boolean                                                                                  | false    | false                     |
| Style            | Indicates whether response body should be wrapped with status code and error message (`Comprehensive`)  or just returned as is                                                                    | enum: `Basic`, `Comprehensive`                                                           | false    | Basic                     |
| Field    | ResponseBody field for `Comprehensive` Style                                                                                                                                                      | string in the UperCamelCase format                                                       | false    | ResponseBody              |
| Namespace        | Mapping between Selector prefix into the View name                                                                                                                                                | string -> string map / pairs                                                             | false    | null                      |
| Visitor          | Interceptor that can execute some logic before or/and after data was fetched from database. In order to use Visitors, Visitor need to be created programmatically and passed to the Configuration | [Visitor](./README.md#Visitor)                                                           | false    | null                      |
| View             | View configuration used to fetch data from database                                                                                                                                               | [View](../view/README.md#View)                                                           | true     | null                      |
| Compression      | Route specific Compression configuration                                                                                                                                                          | [Compression](./README.md#Compression)                                                   | false    | null                      |
| Cache            | Route specific Cache configuration                                                                                                                                                                | [Cache](./README.md#Cache)                                                               | false    | null                      |
| Exclude          | Fields that will be excluded from response.                                                                                                                                                       | Field paths in format: CammelCase.CammelCase.OutputCase, i.e. - Employees.Departments.id | false    | []string{}                |
| NormalizeExclude | In order to use Excluded path using only CammelCase NormalizeExclude needs to be set to false.                                                                                                    | bool                                                                                     | false    | true                      |

### Cache

Cache caches the database result for the main view specified on the Route level. It uses the Selectors to produce entry
key. The cache key is produced using the Selectors. If two http requests produces the same Selectors, and one happen
after the other in time shorter than specified, the data will be read from the cache:

| Section      | Description                                           | Type   | Required |
|--------------|-------------------------------------------------------|--------|----------|
| TimeToLiveMs | Cache entry time after when entry will be invalidated | int    | true     |
| StorageURL   | URL of the stored cache entries                       | string | true     |

### Visitor

Visitor intercepts regular reader flow. Visitor executes regular golang code so in order to use them they have to be
registered before Resource is initialized. It can implement following interfaces:

* BeforeFetch - executes before data is fetched from the database.

```go
BeforeFetch(response http.ResponseWriter, request *http.Request) (responseClosed bool, err error)
```

* AfterFetch - executes after data is fetched from the database.

```go
AfterFetch(data interface{}, response http.ResponseWriter, request *http.Request) (responseClosed bool, err error)
```

| Section    | Description                                                           | Type   | Required |
|------------|-----------------------------------------------------------------------|--------|----------|
| Name       | Visitor name, has to match visitors map key provided programmatically | string | true     |

### Examples

For examples see [test cases](../router/testdata)