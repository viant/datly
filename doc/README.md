# Datly 


Datly has been design as modern flexible ORM for rapid development. 
Datly can operate in  **managed** , **autonomous** and **custom mode**.
In managed mode datly is used as regular GoLang ORM where you operate on golang struct and datly services programmatically.
In autonomous mode datly uses a **dsql** based rules with single gateway entry point handling all incoming request matching defined rules.
In custom mode datly also operates as single gateway entry point handling all incoming request, allowing
method/receiver go struct behaviour customization associated with the rule, this is achieved by either golang
plugins or/and custom type registry integration.
Both _autonomous_ and _custom_ mode datly can be deployed as standalone app or as Docker, Kubernetes, Cloud Serverless runtimes (lambda,GCF,Cloud Run).

### Introduction

Datly promotes using datly SQL (dsql) dialect to define CRUD operations. 
DSQL accept a SQL supported by specific database vendor on top of that it provide hints and template language using [Velty Engine](https://github.com/viant/velty) .
To protect against SQL Injection any input $Variable reference is converted to SQL driver placeholder.

Take the following snippet example
```sql
  INSERT INTO MY_TABLE(ID, NAME) ($Entity.ID, $Entity.Name)
```
will be replaced before calling database driver with
```sql
  INSERT INTO MY_TABLE(ID, NAME) (?, ?)
```

Input variable(s) can be also be accessed with $Unsafe namespace ($Unsafe.MyVariable), in that case variable is inlined.


## The hints helps customize various aspect of data mapping/routing

### Reader hints
- **RouteConfig** is JSON representation of [Route](option/route.go) settings i.e {"URI":"app1/view1/{Id}"}
- **OutputConfig** is JSON representation of [Output](option/output.go) settings i.e {"Style":"Comprehensive"}
- **ColumnConfig** is JSON representation of [Column](option/column.go) settings i.e {"DataType":"bool"}
- **ViewConfig**  is JSON representation of [View](option/view.go) settings i.e {"Cache":{"Ref":"aerospike"}}

### Executor hints

- **View Parameter Hints** defines SQL based data view parameter
```#set($_ = $Records /* 
  SELECT * FROM MY_TABLE /* {"Selector":{}} */ WHERE ID = $Entity.ID
  */)
 ```

## [Velty Tamples](https://github.com/viant/velty)
Datly has ability to dynamically customize both reader and executor service with templates.


## Reader

The reader service is used to retrieve specific pieces of data or to search for data that meets certain criteria. 
The specific pieces of data is defined as single or multi relational view, where each view originate from a table, query or
**Velty Templates**
The reader service allows client to control additional functionality, such as sorting, filtering, and selecting column, formatting the data in a way that is easy to read and analyze.


### Datly SQL (DSQL) 

Datly uses specific dialect of SQL to define rule for view(s) and relation between them.

DSQL is transformed into datly internal view representation with the following command:

```go
datly -C='myDB|driver|dsn' -X myRule.sql [-w=myProjectLocation ]
```
where -w would persist rule with datly config to specific myProjectLocation

Once datly rules are stored, you can start datly with datly -c=myProjectLocation/Datly/config.json


#### Managed mode

In manage mode you use directly reader.Service, with provided view and underlying go struct.

```go
package mypkg

type Invoice struct {
	Id           int32      `sqlx:"name=id"`
	CustomerName *string    `sqlx:"name=customer_name"`
	InvoiceDate  *time.Time `sqlx:"name=invoice_date"`
	DueDate      *time.Time `sqlx:"name=due_date"`
	TotalAmount  *string    `sqlx:"name=total_amount"`
	Items        []*Item
}

type Item struct {
	Id          int32   `sqlx:"name=id"`
	InvoiceId   *int64  `sqlx:"name=invoice_id"`
	ProductName *string `sqlx:"name=product_name"`
	Quantity    *int64  `sqlx:"name=quantity"`
	Price       *string `sqlx:"name=price"`
	Total       *string `sqlx:"name=total"`
}

func ExampleService_ReadDataView() {

	aReader := reader.New()
	conn := aReader.Resource.AddConnector("dbName", "database/sql  driverName", "database/sql dsn")

	invoiceView := view.NewView("invoice", "INVOICE",
		view.WithConnector(conn),
		view.WithCriteria("id"),
		view.WithViewType(reflect.TypeOf(&Invoice{})),
		view.WithOneToMany("Items", "id",
			view.NwReferenceView("", "invoice_id",
				view.NewView("items", "invoice_list_item", view.WithConnector(conn)))),
	)

	aReader.Resource.AddViews(invoiceView)
	if err := aReader.Resource.Init(context.Background()); err != nil {
		log.Fatal(err)
	}

	var  invoices= make([]*Invoice, 0)
	if err := aReader.ReadInto(context.Background(), "invoice", &invoices, reader.WithCriteria( "status = ?",1));err != nil {
		log.Fatal(err)
	}
	invociesJSON, _:=json.Marshal(invoices)
	fmt.Printf("invocies: %s\n", invociesJSON)

	
}


```

See [Reader Service](../reader/README.md) for more details


#### Autonomous mode


##### DSQL structure 

```sql
[RouteConfig]
SELECT mainViewAlias.*  [EXCEPT COLUMN][OutputConfig]
[, secondViewAlias.*       [OutputConfig]  ]
[, NviewAlias.*            [OutputConfig]  ]
FROM (
    SELECT
    ID  [ColumnConfig],
    ...,
     other_column   
    FROM table1
    ) mainViewAlias [ViewConfig],

[
 JOIN (
    SELECT OTHER_ID,
        ...,
        other_column
    FROM table2
    ) secondViewAlias  [ViewConfig] ON mainViewAlias.ID = secondViewAlias.OTHER_ID
    
]    
```

Where
- **RouteConfig** is JSON representation of [Route](option/route.go) settings i.e {"URI":"app1/view1/{Id}"}
- **OutputConfig** is JSON representation of [Output](option/output.go) settings i.e {"Style":"Comprehensive"}
- **ColumnConfig** is JSON representation of [Column](option/column.go) settings i.e {"DataType":"bool"}
- **ViewConfig**  is JSON representation of [View](option/view.go) settings i.e {"Cache":{"Ref":"aerospike"}}

See e2e [testcase](../e2e/local/regression/cases) for more examples


##### Usage

###### One to many

**rule.sql**
```sql
SELECT 
    dept.*
    employee.*
FROM DEPARMENT dept
JOIN EMP employee ON dept.ID = employee.DEPT_ID 
```

```bash
datly -N=dept -X=rule.sql -C='mydb|mydb_driver|mydb_driver_dsn' 
```


###### One to one relation

**rule.sql**
```sql
SELECT 
    dept.*
    employee.*,
    organization.*
FROM DEPARMENT dept
JOIN EMP employee ON dept.ID = employee.DEPT_ID
JOIN ORG organization ON organization.ID = demp.ORG_ID AND 1=1
```

```bash
datly -N=dept -X=rule.sql  -C='mydb|mydb_driver|mydb_driver_dsn' 
```

###### Excluding output column

**rule.sql**
```sql
SELECT 
    dept.* EXCEPT ORG_ID
    employee.* EXCEPT DEPT_ID, 
    organization.* 
FROM DEPARMENT dept
JOIN EMP employee ON dept.ID = employee.DEPT_ID
JOIN ORG organization ON organization.ID = demp.ORG_ID AND 1=1
```

```bash
datly -N=dept -X=rule.sql  -C='mydb|mydb_driver|mydb_driver_dsn' 
```


###### View SQL


**rule.sql**
```sql
SELECT 
    dept.* EXCEPT ORG_ID
    employee.* EXCEPT DEPT_ID, 
    organization.* 
FROM (SELECT * FROM DEPARMENT t) dept
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee ON dept.ID = employee.DEPT_ID
JOIN ORG organization ON organization.ID = demp.ORG_ID AND 1=1
```

```bash
datly -N=dept -X=rule.sql  -C='mydb|mydb_driver|mydb_driver_dsn' 
```


###### View SQL with velty template and query parameters

```sql
SELECT 
    dept.* EXCEPT ORG_ID
    employee.* EXCEPT DEPT_ID, 
    organization.* 
FROM (SELECT * FROM DEPARMENT t) dept
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee ON dept.ID = employee.DEPT_ID
JOIN ORG organization ON organization.ID = demp.ORG_ID AND 1=1
WHERE 1=1
#if ($Has.Id)
AND ID = $Id
#end
```

###### View SQL with query parameters

```sql
SELECT 
    dept.* EXCEPT ORG_ID
    employee.* EXCEPT DEPT_ID, 
    organization.* 
FROM (SELECT * FROM DEPARMENT t) dept
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee ON dept.ID = employee.DEPT_ID
JOIN ORG organization ON organization.ID = demp.ORG_ID AND 1=1
WHERE ID = $Id
```

###### View SQL column type codec

```sql
SELECT 
    dept.* EXCEPT ORG_ID
    employee.* EXCEPT DEPT_ID, 
    organization.* 
FROM (SELECT * FROM DEPARMENT t) dept
JOIN (SELECT ID, NAME, DEPT_ID, 
    (CASE WHEN COLUMN_X = 1 THEN
            'x1,x2'
             WHEN COLUMN_X = 2 THEN
            'x3,x4'
       END) AS SLICE /* {"Codec":{"Ref":"AsStrings"}, "DataType": "string"} */  
    FROM EMP t) employee ON dept.ID = employee.DEPT_ID
JOIN ORG organization ON organization.ID = demp.ORG_ID AND 1=1
WHERE ID = $Id
```

###### Supported conversion Codecs
    - AsStrings: converts coma separated value into []string


###### Setting matching URI

```sql
/* {"URI":"dept/"} */
SELECT
dept.* EXCEPT ORG_ID
employee.* EXCEPT DEPT_ID
FROM (SELECT * FROM DEPARMENT t) dept               
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee 
 ON dept.ID = employee.DEPT_ID
```


###### Setting data caching

```sql
/* {"URI":"dept/", 
   "Cache":{
         "Name": "fs"
         "Location": "/tmp/cache/${view.Name}",
         "TimeToLiveMs": 360000
         }
   } */
SELECT
dept.* EXCEPT ORG_ID
employee.* EXCEPT DEPT_ID
FROM (SELECT * FROM DEPARMENT t) dept                /* {"Cache":{"Ref":"fs"}} */
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee  /* {"Cache":{"Ref":"fsgit p"}} */
 ON dept.ID = employee.DEPT_ID
```


```sql
/* {"URI":"dept/", 
   "Cache":{
         "Name": "aerospike",
         "Provider": "aerospike://127.0.0.1:3000/test",
         "Location": "${view.Name}",
         "TimeToLiveMs": 360000
         }
   } */
SELECT
dept.* EXCEPT ORG_ID
employee.* EXCEPT DEPT_ID
FROM (SELECT * FROM DEPARMENT t) dept                /* {"Cache":{"Ref":"aerospike"}} */
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee  /* {"Cache":{"Ref":"aerospike"}} */
 ON dept.ID = employee.DEPT_ID
```

###### Setting selector

```sql
SELECT
dept.* EXCEPT ORG_ID
employee.* EXCEPT DEPT_ID
FROM (SELECT * FROM DEPARMENT t) dept                /* {"Selector":{"Limit": 40, "Constraints"{"Criteria": false}}} */
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee  /* {"Selector":{"Limit": 80, "Constraints"{"Criteria": false, "Limit": false, "Offset": false}}} */
 ON dept.ID = employee.DEPT_ID
```


### Authentication

### Authorization

##  Executor

Executor service is used to validate, transform and modify data in database programmatically.
Post (insert), Put(update), Patch(insert/update) and Delete operation are supported.  


#### Autonomous mode

To generate initial executor DSQL datly would use regular reader DSQL defining
one or multi view with corresponding relations with additional input hints
All DML operation are executed in the one transaction, any errors trigger either 
by database or programmatically  ($logger.Fatalf) cause transaction rollback.

Executor service can accept input in the following form:

- **simple object ({})** 
```sql 
     SELECT myTable.* /* { "Cardinality": "One" } */
    FROM (SELECT * FROM MY_TABLE) myTable
```

- **simple namespaced object i.e. {"Data": {}}**
```sql 
     SELECT myTable.* /* { "Cardinality": "One", "Field":"Data" } */
    FROM (SELECT * FROM MY_TABLE) myTable
```

- **array objects ([])** 
```sql 
  SELECT myTable.* /* { "Cardinality": "Many" } */
  FROM (SELECT * FROM MY_TABLE) myTable
```

- **array namespace objects {"Data": [{}]}**
```sql 
  SELECT myTable.* /* { "Cardinality": "Many" , "Field":"Data"} */
  FROM (SELECT * FROM MY_TABLE) myTable
```

```go
datly -C='myDB|driver|dsn' -X myRule.sql -G=patch|post|put|delete [--dsqlOutput=dslOutpuPath] [--goFileOut=goFileOutPath]
```

As a result the following file would be generated:
- dsql/<myRule>.sql  - initial logic for patch|post|put|delete operations
- dsql/<myrule>.go   - initial go struct(s)   
- dsql/<myrule>Post.json - example of JSON for testing a service

Generated go struct(s) can be modified with additional tags.

Datly uses 'validate' and 'sqlx' tags to control input validation. 

Datly generate basic tempalte with the following parameters expressions

- #set($_ = $Entity<*Entity>(body/))  for simple object ({})
- #set($_ = $Entities<[]*Entity>(body/))  for simple array ([])
- #set($_ = $Entity<*Entity>(body/data))  for namespaced object ({"data":{}})
- #set($_ = $Entities<[]*Entity>(body/data))  for namespaced array ({"data":[]})


After adjusting logic in executor dsql, 
datly -C='myDB|driver|dsn' -X exeuctor_dsql_rule.sql --relative=mystructgopath -w=mydatlyproject


For simplicity it's recommended to leave in velhy tempalte just a bare data read/write wiring, thus
delegate validation and initialization with previous state to go code and simple call from dsql.

For example:






##### Executor DSQL 

```dsql
/* ROUTE OPTION */
import ...
#set( $_ = ...) input paramter initialization

 DML | velocity expr (#set|#if|#foreach)

```
#### Supported build in functions:

#### Logger/Formatter
- $logger.FatalF
- $logger.LogF
- $logger.PrintF
- $fmt.Sprintf

#### SQL
- $sequencer.Allocate(tableName string, dest interface{}, selector string) 
- $sqlx.Validate

#### Message bus
Universal message bus, provide ability to send/publish asyn message a message bus (i.e sqs/sns,pubsub,kafka) 
 - $messageBus.Message creates a message
 - $messageBus.Push push a message
 - 
```vm
 - #set($msg = $messageBus.Message("aws/topic/us-west-1/mytopic", $data))
  #set($confirmation = $messageBus.Push($msg))
  $logger.Printf("confirmation:%v", $confirmation.MessageID)
```

#### Validator
 - $sqlx.Validate - validates a struct with sqlx tags
 - $validator.Validate - validates struct with validate tag

#### HTTP
- $http.Do
- $http.Get
- $response.Failf
- $response.FailfWithStatusCode
- $response.StatusCode

#### Comparators
- $differ.Diff

TODO add all supported and update/add example


###### Validation

Any database constraint validation can be customized with [sqlx validator service](https://github.com/viant/sqlx#validator-service)
```dsql
#set($validation = $sqlx.Validate($Entity))
#if($validation.Failed)
  $logger.Fatal($validation)
#end
```


###### Insert operation:

with service
```dsql
$sequencer.Allocate("MY_TABLE", $Entity, "Id")
#if($Unsafe.Entity)
  $sql.Insert($Entity, "MY_TABLE");
#end
```

with DML
```dsql
$sequencer.Allocate("MyTable", $Entity, "Id")
INSERT INTO MY_TABLE(ID, NAME) VALUES($Entity.Id, $Entity.Name)
```


###### Update operation:

```dsql
$sequencer.Allocate("MyTable", $Entity, "Id")
#if($Unsafe.Entity)
  $sql.Update($Entity, "MyTable");
#end
```


with DML
```dsql
UPDATE MY_TABLE SET 
    NAME = $Entity.Name
#if($Entity.Has.Description)
    , DESCRIPTION = $Entity.Description
#end
WHERE ID = $Entity.Id
```

###### Importing go types

```sql

/* {"Method":"PATCH","ResponseBody":{"From":"Product"}} */

import (
	"./product.Product"
)
#set($_ = $Jwt<string>(Header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Campaign<*[]Product>(body/Entity))
```


###### Blending go call within dsql

```sql

/* {"Method":"PATCH","ResponseBody":{"From":"Product"}} */

import (
	"./product.Product"
)
#set($_ = $Jwt<string>(Header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Campaign<*[]Product>(body/Entity))

#set($validation = $New("*Validation"))
#set($hasError = $Product.Init($validation))

....

#set($hasError = $Product.Validate($validation))

```


###### Fetching existing data with data view parameters

```sql
#set($_ = $Records /*  
  SELECT * FROM MY_TABLE  WHERE ID = $Entity.ID
*/)
```

Data view parameters use regular reader DSQL and can return one or more records.

By default all parameters are required,  adding '?' character before SELECT keyword would make parameter option, 
to 
```sql
#set($_ = $Records /*  
  ? SELECT * FROM MY_TABLE  WHERE ID = $Entity.ID
*/)
```

To specify required parameter with error code you can simple add !3DigitErrotCode 

```sql
#set($_ = $Records /*  
  !401 SELECT * FROM MY_TABLE  WHERE ID = $Entity.ID
*/)
```



###### Fetching data with StructQL

- [StructQL](https://github.com/viant/structql)


```sql

import (
	"./product.Product"
)

#set($_ = $Products<*[]Product>(body/Data))

#set($_ = $ProductsIds<?>(param/Products) /* 
   SELECT ARRAY_AGG(Id) AS Values FROM  `/`
*/)

#set($_ = $prevProducts<*[]Product>()/*
SELECT * FROM Products WHERE $criteria.In("ID", $ProductsIds.Values) 
*/)
```
In the example above in the first step collection of products is defined from POST body data field.
Second parameter extract all products Id with structql, in the final prevProducts fetches all produces
where ID is listed in ProductsIds parameters.
Note that we use $criteria.In function to automatically generate IN statement if parameter len is greater than zero
otherwise the criteria.In function returns false, to ensure correct SQL generation and expected behaviours


###### Indexing data

Any go collection can be index with IndexBy dsql method 

```sql

#set($_ = $Records /*
  SELECT * FROM MY_TABLE
*/)


#set($ById = $Records.IndexBy("Id"))

#foreach($rec in $Unsafe.$Entities)
    #if($ById.HasKey($rec.Id) == false) 
        $logger.Fatal("not found record with %v id", $rec.Id) 
    #end
    #set($prev = $ById[$rec.Id])
#end

```



###### Authentication & Authorization


```sql
#set($_ = $Jwt<string>(Header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Authorization  /*
    {"Type": "Authorizer", "StatusCode": 403}
    SELECT Authorized /* {"DataType":"bool"} */
    FROM (SELECT IS_VENDOR_AUTHORIZED($Jwt.UserID, $vendorID) AS Authorized) t
    WHERE Authorized
*/)
```



###### Record Differ

```sql
#set($_ = $Records /* {"Required":false}
  #set($Ids = $Entities.QueryFirst("SELECT ARRAY_AGG(Id) AS Vals FROM  `/`"))
  SELECT * FROM MY_TABLE /* {"Selector":{}} */
  WHERE  #if($Ids.Vals.Length() > 0 ) ID IN ( $Ids.Vals ) #else 1 = 0 #end */
)

#set($ById = $Records.IndexBy("Id"))

#foreach($rec in $Unsafe.$Entities)
    #if($ById.HasKey($rec) == false) 
        $logger.Fatal("not found record with %v id", $rec.Id) 
    #end
    #set($prev = $ById[$rec.Id])

    #set($recDiff = $differ.Diff($prev, $rec))
    #if($fooDif.Changed())
        INSERT INTO DIFF_JN(DIFF) VALUES ($recDiff.String());
    #end
#end
```

### Meta repository

#### OpenAPI

#### Internal View

#### Go Struct


#### Performance metrics


### Plugin architecture && Custom Datly

[Data extension](extension/README.md)


### Caching architecture

#### Lazy Mode

#### Smart Mode


## Deployment

Datly is runtime agnostic and can be deployed as standalone app, or AWS Lambda, Google cloud function, Google Cloud run.
Entry point with deployment example are define under [Runtime](../gateway/runtime)

Datly deployment is entails datly binary deployment with initial rule set, follow by just rule synchronization.


On both autonomous and custom mode 'datly' uses set of rule, and plugins. 
On cloud deployment these assets are stored on cloud storage, thus to reduce cold start or rule changes detection and reload
it's recommend to set flag "UseCacheFS" in the datly config. This setting instructs daytly to use **datly.pkg.gz** cache file, for all underlying assets. 
Cache is created every time a cache file is deleted from a file storage.

#### Generating pre-packaged datly rule  

While building cache file with hundreds rules and assets cache file provides both cost and performance optimization on cloud storage, 
to prepackage  **datly rule** ahead of time run the following command:


```bash
    datly -P DATLY_ROOT_CONFIG -R CLOUD_STORAGE_DATLY_CONFIG_URL
  
    i.e  datly -P /opt/ws/Datly -R s3://myog-serverless-config/Datly
```

The above command creates datly.pkg.gz file containing all assets from DATLY_ROOT_CONFIG location, 
where each asset URL is rewritten with CLOUD_STORAGE_DATLY_CONFIG_URL


#### Project layout

The following layout organizes datly specific resources
```bash
  ProjectRoot
      | -  dsql
            | - business Unit 1 (appName)
                 | - entity_X_get.sql
                 | - entity_X_put.sql 
                 | - entity_X_post.sql 
                 | - entity_X_patch.sql 
                ....
                 | - entity_N_get.sql
                 | - routerY.rt   
                 | - entity_N
                      - other_asset.xsd
                      
    
            | - business Unit N (appName)       
                 | - entityM_get.sql
                 ...
                 | - routerY.rt            
   - e2e (end to end testing workflows)
   - pkg         
      | -  mypackage1(business Unit 1)   
      |       | - entityX.go
      | -  mypackageN(business Unit Y)
              |  - ...      
   - deployment 
      - prod
         | - Datly
               | - dependencies
               | - plugins
               | - routes
               |  config.json
      - stage        
         | - Datly
               | - dependencies
               | - plugins
               | - routes
               |   config.json   
```


#### Securing secret

Datly integrates with [Scy - secure store api](https://github.com/viant/scy) when operating on credentials.


#### Securing database/sql DSN

In **dependencies** folder datly stores connection details make sure that before deploying to stage/prod all
credentials details are replaced with the following macros

```connections.yaml
Connectors:
    - DSN: ${Username}:${Password}@tcp(${Endpoint}:3306)/ci_ads?parseTime=true
      Driver: mysql
      Name: mydb
      Secret:
        URL: secure_storage_url
        Key:  blowfish://default
  - DSN: bigquery://my_org_project/myDataset?credURL=url_encoded_secure_storage_N_url
    Driver: bigquery
    Name: mybqdb
```

Where secure_storage_url could be any file system, including secret storage manager
    - AWS SecretManager i.e. aws://secretmanager/us-west-2/secret/myorg/datly/e2e/mysql/mydb
    - AWS SystemManager i.e. aws://ssm/us-west-1/parameter/MyOrgDatlyE2eMySQLMyDb
    - GCP SecretManager i.e. gcp://secretmanager/projects/myorf-e2e/secrets/mysqlMyDB


To secure database credentials create [raw_credentials.json](asset/raw_credentials.json) file
and the use the following [scy](https://github.com/viant/scy) command

```bash
scy -s=raw_credentials.json -d=secure_storage_url -t=basic -k=blowfish://default
```

To secure Google Service Account Secret use the following [scy](https://github.com/viant/scy) command

```bash
scy -s=myServiceAccountSecret.json -d=secure_storage_url -t=raw -k=blowfish://default
```

#### Autonomous Datly

To build standalone binary:
```bash
git clone https://github.com/viant/datly.git
cd datly/cmd/datly
go build
datly -h
```

To build datly for  Docker or cloud specific[Runtimes](../gateway/runtime) 
check **deploy.yaml** [endly](https://github.com/viant/endly) deployment workflows.


#### Custom Datly

In custom datly (xdatly) mode you get integrated with your local go module to define application specific type with method, so these method can be invoked directly from dsql.
In this scenario datly uses both direct go module integration and go plugin to synchronize dynamic rules without need of rebuilding custom datly.








#### Executing rule in go program & debugging

Datly is purely written and go, and thus it's possible to take any rule and load it and run it as if it was
defined in the managed mode, for hava breakpoint on any rule to go call methods.

```go
//If you create rule for executor service (PATH/PUT/POST) you can execute and debug it in the pure golang.
func Example_RuleExecution() {

	//Uncomment various additional debugging option and debugging and troubleshooting
	// expand.SetPanicOnError(false)
	// read.ShowSQL(true)
	// update.ShowSQL(true)
	// insert.ShowSQL(true)

	ctx := context.Background()

	service := datly.New(datly.NewConfig())
	viewName := "product"
	ruleURL := fmt.Sprintf("yyyyyyy/Datly/routes/dev/%v.yaml", viewName)
	err := service.LoadRoute(ctx, ruleURL,
		view.NewPackagedType("domain", "Product", reflect.TypeOf(Product{})),
		view.NewPackagedType("domain", "Validation", reflect.TypeOf(Validation{})),
	)
	if err != nil {
		log.Fatal(err)
	}
	err = service.Init(ctx)
	if err != nil {
		log.Fatal(err)
	}

	//Create http Patch request for example
	URL, _ := surl.Parse("http://127.0.0.1:8080/v1/api/dev")
	httpRequest := &http.Request{
		URL:    URL,
		Method: "PUT",
		Body:   io.NopCloser(strings.NewReader(`{"Entity":{"VendorId":5672}}`)),
		Header: http.Header{},
	}
	token, err := service.JwtSigner.Create(time.Hour, &jwt.Claims{
		Email:  "dev@viantinc.com",
		UserID: 111,
	})
	if err != nil {
		log.Fatal(err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+token)
	routeRes, _ := service.Routes()
	route := routeRes.Routes[0] //make sure you are using correct route
	err = service.Exec(ctx, viewName, datly.WithExecHttpRequest(ctx, route, httpRequest))
	if err != nil {
		log.Fatal(err)
	}
}

```