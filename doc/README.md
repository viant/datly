# Datly 

Datly has been design as modern flexible ORM for rapid development. Datly can operate in 
managed, autonomous and custom mode.
In managed mode datly is used as regular GoLang ORM where a user operate on golang struct and datly services programmatically.
In autonomous mode datly operates as single gateway entry point handling all incoming request  with corresponding rules.
In custom mode datly also operates as single gateway entry point handling all incoming request, allowing
method/receiver go struct behaviour customization associated with the rule, this is achieved by either golang
plugins or/and custom type registry integration.

Both autonomous and custom mode datly can be deployed as standalone app or as Docker, Kubernetes, Cloud Serverless runtimes (lambda,GCF,Cloud Run).


## [Velty Tamples](https://github.com/viant/velty)
Datly has ability to dynamically customize both reader and executor service with templates.

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

#### Managed mode

TODO some more example here.


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
datly -N=dept -X=rule.sql
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
datly -N=dept -X=rule.sql
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
datly -N=dept -X=rule.sql
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
datly -N=dept -X=rule.sql
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

Generated input variable can be one of the following form
- #set($_ = $Entity<*Entity>(body/))  for simple object ({})
- #set($_ = $Entities<[]*Entity>(body/))  for simple array ([])
- #set($_ = $Entity<*Entity>(body/data))  for namespaced object ({"data":{}})
- #set($_ = $Entities<[]*Entity>(body/data))  for namespaced array ({"data":[]})

##### Executor DSQL 

```dsql
/* ROUTE OPTION */
import ...
#set( $_ = ...) input paramter initialization
 DML | velocity expr (#set|#if|#foreach)

```
Supported build in functions:
- $logger.FatalF
- $logger.LogF
- $logger.PrintF
- $differ.Diff
- $sqlx.Validate
- $http.Do
- $http.Get

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


###### Fetching existing data

```sql
#set($_ = $Records /* {"Required":false} 
  SELECT * FROM MY_TABLE /* {"Selector":{}} */ WHERE ID = $Entity.ID
*/)
```

###### Fetching data with StructQL

- [StructQL](https://github.com/viant/structql)

```sql
#set($_ = $Records /* {"Required":false} 
  #set($Ids = $Entities.QueryFirst("SELECT ARRAY_AGG(Id) AS Values FROM  `/`"))
  SELECT * FROM MY_TABLE /* {"Selector":{}} */
  WHERE  #if($Ids.Values.Length() > 0 ) ID IN ( $Ids.Values ) #else 1 = 0 #end 
*/)
```


```sql
#set($_ = $InvoiceLines /* {"Required":false}
#set($Ids = $Invoice.QueryFirst("SELECT ARRAY_AGG(Id) AS Values FROM  `/LineItems/`"))
SELECT * FROM INVOICE_LINE_ITEM /* {"Selector":{}} */
WHERE  #if($Ids.Values.Length() > 0 ) ID IN ( $Ids.Values ) #else 1 = 0 #end 
*/)
```


###### Indexing data

```sql
#set($_ = $Records /* {"Required":false}
  #set($Ids = $Entities.QueryFirst("SELECT ARRAY_AGG(Id) AS Vals FROM  `/`"))
  SELECT * FROM MY_TABLE /* {"Selector":{}} */
  WHERE  #if($Ids.Vals.Length() > 0 ) ID IN ( $Ids.Vals ) #else 1 = 0 #end */
)

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
#set($_ = $Acl /*  {"Auth":"Jwt"}  
    SELECT ID, STATUS, (IS_ENTITY_AUTHORIZED($Jwt.UserID, ID)) AS IS_AUTH FROM MY_TABLE WHERE ID = $Entity.Id
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

### Caching architecture

#### Lazy Mode

#### Smart Mode




