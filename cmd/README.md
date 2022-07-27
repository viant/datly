## Command line datly executor


```go
./datly -h
```

#### Generate rule with endpoint for a table

```bash
 ./datly  -C=dev  -N=MyViewName -T=MyTableName
 
open http://127.0.0.1:8080/v1/api/dev/MyViewName

```

#### Generate rule with endpoint for a table and SQL

```bash
 ./datly  -C=dev  -N=MyViewName -T=MyTableName -S=view.sql
```

```bash
 ./datly  -C=dev  -N=MyViewName -T=MyTableName
```

#### Generate rule with endpoint for a table and relations

```bash
 ./datly  -C=dev  -N=MyViewName -T=MyTableName  -R=MyRelName:RelTable 
```

##### SQLx (extension) based rule generation

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


##### Setting matching URI

```sql
/* {"URI":"dept/"} */
SELECT
dept.* EXCEPT ORG_ID
employee.* EXCEPT DEPT_ID
FROM (SELECT * FROM DEPARMENT t) dept               
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee 
 ON dept.ID = employee.DEPT_ID
```


#### Setting data caching

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
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee  /* {"Cache":{"Ref":"fs"}} */
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


### Setting selector

```sql
SELECT
dept.* EXCEPT ORG_ID
employee.* EXCEPT DEPT_ID
FROM (SELECT * FROM DEPARMENT t) dept                /* {"Selector":{"Limit": 40, "Constraints"{"Criteria": false}}} */
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee  /* {"Selector":{"Limit": 80, "Constraints"{"Criteria": false, "Limit": false, "Offset": false}}} */
 ON dept.ID = employee.DEPT_ID
```


#### Persisting routes/config to the local folder

Use -w=location switch

```sql
datly -N=dept -T=DEPT -w=my_project
```

