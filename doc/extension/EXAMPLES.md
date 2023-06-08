# How-to Guides
## General prerequisites
### Datly

+ Download [Datly](https://github.com/viant/datly) project
+ Build datly
```shell
cd <folder_contains_datly_project>/datly/cmd/datly
go build
mv datly /usr/local/bin
```

### MySQL database
+ For this guide purposes I'm using MySQL docker image preloaded with the Sakila example database.
+ [Git project](https://github.com/sakiladb/mysql) (check version for amd64 or arm64 platform)
+ [Sakila database doc](https://dev.mysql.com/doc/sakila/en/)
+ [The sakila Database EER Diagram](https://dev.mysql.com/doc/workbench/en/wb-documenting-sakila.html#wb-sakila-eer-image)

```shell
docker run -p 3306:3306 --name sakiladb -d sakiladb/mysql:latest
```
+ By default these are created
    + database: sakila
    + users: root, sakila
    + password: p_ssW0rd

## 1 Basic patch service for actor table - standalone app in custom mode 

### 1.1 Generate project
   ```shell
   datly initExt -p=~/myproject -n=mymodule
   ```
The following project structure get generated

```bash
 myproject
  | - .build
  |    | - datly
  |    | - ext
  |
  | - dsql
  | 
  | - pkg 
  |    | - bootstrap
  |    |    | - bootstrap.go
  |    |
  |    | - checksum
  |    |    | - init.go
  |    |
  |    | - dependency
  |    |    | - init.go
  |    |
  |    | - go.mod
  |    | - xinit.go
```

### 1.2 Create folder for actor's resources
   ```shell
   mkdir -p ~/myproject/dsql/actor/init
   ```

### 1.3 Create actor's init sql file
   ```shell
   touch ~/myproject/dsql/actor/init/actor_init.sql
   ```
```shell
 myproject
  ...
  | - dsql 
  |    | - actor
  |         | - init
  |              | - actor_init.sql
  ...
```

### 1.4 Add content to actor_init.sql

e.g. for vi users
```shell
vi ~/myproject/dsql/actor/init/actor_init.sql
```

```sql
/* { "URI":"actor"} */
SELECT  Actor.* /* {"Cardinality":"Many", "Field":"Entity" } */
FROM (select * from actor) Actor
```

### 1.5 Generate go files and dsql file for patch operations and JSON entity
```shell
datly gen \
-o=patch \
-c='sakiladb|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila?parseTime=true' \
-s=dsql/actor/init/actor_init.sql \
-g=actor \
-p=~/myproject
```

The following folders and files get generated or updated
```shell
 myproject
  ...
  | - dsql 
  |    | - actor
  |         | - Actor.sql
  |         | - EntityPost.json
  |      
  | - pkg 
  |    | - actor
  |    |    | - entity.go 
  |    |
  |    | - dependency
  |         | - init.go
  ...
```
+ Actor.sql - patch logic written in dsql
+ EntityPost.json - request body template just with required fields
+ entity.go - all needed go structs
+ init.go - updated imports

### 1.6 Initialise datly rule repository
```shell
datly init -p=~/myproject \
-c='sakiladb|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila?parseTime=true' \
-r=repo/dev
```
The following folders and files get generated
```shell
 myproject
  ...
  | - repo
  |    | - dev
  |         | - Datly 
  |              | - assets
  |              | - dependencies
  |              |    | - connections.yaml
  |              |
  |              | - routes
  |              | - config.json 
  ...
```

+ connections.yaml - contains defined connectors:
```yaml
Connectors:
    - DSN: root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila?parseTime=true
      Driver: mysql
      Name: sakiladb
ModTime: "2023-06-02T20:16:54.658521+02:00"
```

+ **Tip**: You can init repository with more than 1 connector i.e.:
```shell
datly init -p=~/myproject \
-c='sakiladb01|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila01?parseTime=true' \
-c='sakiladb02|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila02?parseTime=true' \
-r=repo/dev
```
so connections.yaml has content:
```yaml
Connectors:
  - DSN: root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila01?parseTime=true
    Driver: mysql
    Name: sakiladb01
  - DSN: root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila02?parseTime=true
    Driver: mysql
    Name: sakiladb02
ModTime: "2023-06-02T20:16:54.658521+02:00"
```

First connector in connections.yaml is a default connector for all dsql files.
You can use connector different from default.

**Warning!**  
***Choosing connector option is currently implemented only for reader service.***  
***You can't use it for insert/update/delete operations because they use executor service.***


Add $DB[<connector_name>] param prefix before db source inside dsql file i.e. in ActorReader.sql:
```sql
/* { "URI":"reader/actor"} */
SELECT  actor.*
FROM (select * from $DB[sakiladb02].actor) actor
```

### 1.7 Generate repo rules from dsql
```shell
datly dsql -s=dsql/actor/Actor.sql \
-p=~/myproject \
-r=repo/dev
```
The following folders and files get generated
```shell
 myproject
  ...
  | - repo
  |    | - dev
  |         | - Datly 
  |              | - routes
  |                   | - dev
  |                        | - Actor
  |                        |    | - Actor.sql
  |                        |    | - curActor.sql
  |                        |
  |                        | - Actor.yaml
  ...
```

### 1.8 Build standalone app
linux
```shell
datly build -p=~/myproject -r=standalone -d=~/myproject/bin -o=linux -a=amd64
chmod u+x ~/myproject/bin/datly
```
macos
```shell
datly build -p=~/myproject -r=standalone -d=~/myproject/bin -o=darwin -a=amd64
chmod u+x ~/myproject/bin/datly
```

### 1.9 Run app
```shell
~/myproject/bin/datly run -c=~/myproject/repo/dev/Datly/config.json
```
Result of starting datly:
```shell
[INFO] Build time: 0001-01-01 00:00:00 +0000 UTC
------------ config ------------
         /Users/<USER>/myproject/repo/dev/Datly/config.json {
        "URL": "/Users/<USER>/myproject/repo/dev/Datly/config.json",
        "Version": "",
        "APIPrefix": "/v1/api/",
        "RouteURL": "/Users/<USER>/myproject/repo/dev/Datly/routes",
        "PluginsURL": "/Users/<USER>/myproject/repo/dev/Datly/plugins",
        "DependencyURL": "/Users/<USER>/myproject/repo/dev/Datly/dependencies",
        "AssetsURL": "/Users/<USER>/myproject/repo/dev/Datly/assets",
        "UseCacheFS": false,
        "SyncFrequencyMs": 2000,
        "Secrets": null,
        "JWTValidator": null,
        "JwtSigner": null,
        "Cognito": null,
        "Meta": {
                "AllowedSubnet": null,
                "Version": "",
                "MetricURI": "/v1/api/meta/metric",
                "ConfigURI": "/v1/api/meta/config",
                "StatusURI": "/v1/api/meta/status",
                "ViewURI": "/v1/api/meta/view",
                "OpenApiURI": "/v1/api/meta/openapi",
                "CacheWarmURI": "/v1/api/cache/warmup",
                "StructURI": "/v1/api/meta/struct"
        },
        "AutoDiscovery": true,
        "ChangeDetection": {
                "NumOfRetries": 15,
                "RetryIntervalInS": 60
        },
        "DisableCors": false,
        "RevealMetric": true,
        "CacheConnectorPrefix": "",
        "APIKeys": [
                {
                        "URI": "/v1/api/dev/secured",
                        "Value": "changeme",
                        "Header": "App-Secret-Id",
                        "Secret": null
                }
        ],
        "Endpoint": {
                "Port": 8080,
                "ReadTimeoutMs": 0,
                "WriteTimeoutMs": 0,
                "MaxHeaderBytes": 0
        },
        "Info": {
                "title": "",
                "version": ""
        }
}

[INFO] initialised datly: 17.969712ms
starting endpoint: 8080
```

### 1.10 Add new actor by sending request using e.g. Postman
```http request
PATCH /v1/api/dev/actor HTTP/1.1
Host: 127.0.0.1:8080
Content-Type: application/json
```
```json
{
  "Entity": [
    {
      "actorId":0,
      "firstName": "John",
      "lastName": "Wazowski",
      "lastUpdate": "2023-01-01T00:00:00Z"
    }
  ]
}
```
The response should be like:
```json
[
    {
        "actorId": 201,
        "firstName": "John",
        "lastName": "Wazowski",
        "lastUpdate": "2023-01-01T00:00:00Z"
    }
]
```

### 1.11 Update actor's name inserted before
```http request
PATCH /v1/api/dev/actor HTTP/1.1
Host: 127.0.0.1:8080
Content-Type: application/json
```
```json
{
  "Entity": [
    {
      "actorId": 201,
      "firstName": "Mike"
    }
  ]
}
```
+ **Tip**: We have to pass 4 fields (so far) in the request body because they are all required by db.  
You can simple use ~/myproject/dsql/actor/EntityPost.json file as a template. It contains just required fields.

The response should be like:
```json
[
    {
        "actorId": 201,
        "firstName": "Mike",
        "lastName": "Wazowski",
        "lastUpdate": "2023-06-08T21:13:38+02:00"
    }
]
```

### 1.12 Add InitialiseForInsert method, which will uppercase actor's name
+ add ~/myproject/pkg/actor/init.go file with content:
```go
package actor

import "strings"

func (a *Actor) InitialiseForInsert() bool {
  a.FirstName = strings.ToUpper(a.FirstName)
  return true
}
```
+ **generate plugin**  

linux
```shell
datly plugin -p=~/myproject -r=repo/dev -o=linux -a=amd64
```
macos
```shell
datly plugin -p=~/myproject -r=repo/dev -o=darwin -a=amd64
```
The following folders and files get generated
```shell
 myproject
  ...
  | - repo
  |    | - dev
  |         | - Datly 
  |              | - plugins
  |                   | - main_1_17_1_darwin_amd64.pinf
  |                   | - main_1_17_1_darwin_amd64.so.gz
  ...
```
+ add InitialiseForInsert invocation inside file ~/myproject/dsql/actor/Actor.sql

```code
#if(($curActorByActorId.HasKey($recActor.ActorId) == true))
  $sql.Update($recActor, "actor");
#else
  #set($inited = $recActor.InitialiseForInsert())
  $sql.Insert($recActor, "actor");
#end
```
+ **Generate again repo rules for Actor.sql**
```shell
datly dsql -s=dsql/actor/Actor.sql \
-p=~/myproject \
-c='sakiladb|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila?parseTime=true' \
-r=repo/dev
```

**Tip:**  
You don't have to build and deploy app after changing rules or code in go.
Generated plugins and rules are automatically reloaded by the app on runtime.  

**Warning:**
- You can use only public method's invocations inside DSQL files (in this case Actor.sql)
- Method has to return value.
- Can't use nil as function argument value without variable (inside DSQL file) 

### 1.13 Check if InitialiseForInsert works

```http request
PATCH /v1/api/dev/actor HTTP/1.1
Host: 127.0.0.1:8080
Content-Type: application/json
```
```json
{
  "Entity": [
    {
      "actorId":0,
      "firstName": "John",
      "lastName": "Wazowski",
      "lastUpdate": "2023-01-01T00:00:00Z"
    }
  ]
}
```
The response should be like:
```json
[
    {
        "actorId": 202,
        "firstName": "JOHN",
        "lastName": "Wazowski",
        "lastUpdate": "2023-01-01T00:00:00Z"
    }
]
```

You can also see on app console that the plugin and routes were reloaded:
```text
[INFO] loaded plugin after: 429.497878ms
[INFO] detected resources changes, rebuilding routers
[INFO] routers rebuild completed after: 441.979753ms
```

### 1.14 Add InitialiseForUpdate method that will set lastUpdate and lastName fields
+ add method into ~/myproject/pkg/actor/init.go file:
```go
func (a *Actor) InitialiseForUpdate(cur *Actor) bool {
	firstNameUpper := false

	if a.Has.LastName {
		if a.Has.FirstName {
			firstNameUpper = a.FirstName == strings.ToUpper(a.FirstName)
		} else {
			firstNameUpper = cur.FirstName == strings.ToUpper(cur.FirstName)
		}

		if firstNameUpper {
			a.LastName = strings.ToUpper(a.LastName)
		}
	}

	if !a.Has.LastUpdate {
		a.LastUpdate = time.Now()
		a.Has.LastUpdate = true
	}

	return true
}
```
+ **generate plugin**

linux
```shell
datly plugin -p=~/myproject -r=repo/dev -o=linux -a=amd64
```
macos
```shell
datly plugin -p=~/myproject -r=repo/dev -o=darwin -a=amd64
```

+ add InitialiseForUpdate invocation inside file ~/myproject/dsql/actor/Actor.sql

```code
#if(($curActorByActorId.HasKey($recActor.ActorId) == true))
  #set($inited = $recActor.InitialiseForUpdate($curActorByActorId[$recActor.ActorId]))
  $sql.Update($recActor, "actor");
#else
  #set($inited = $recActor.InitialiseForInsert())
  $sql.Insert($recActor, "actor");
#end
```
+ **Generate again repo rules for Actor.sql**
```shell
datly dsql -s=dsql/actor/Actor.sql \
-p=~/myproject \
-c='sakiladb|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila?parseTime=true' \
-r=repo/dev
```

### 1.14 Check if InitialiseForUpdate works

```http request
PATCH /v1/api/dev/actor HTTP/1.1
Host: 127.0.0.1:8080
Content-Type: application/json
```
```json
{
  "Entity": [
    {
      "actorId":202,
      "lastName": "Biden"
    }
  ]
}
```
The response should be like:
```json
[
    {
        "actorId": 202,
        "firstName": "JOHN",
        "lastName": "BIDEN",
        "lastUpdate": "2023-01-01T00:00:00Z"
    }
]
```

You can also see on app console that the plugin and routes were reloaded:
```text
[INFO] loaded plugin after: 429.497878ms
[INFO] detected resources changes, rebuilding routers
[INFO] routers rebuild completed after: 441.979753ms
```

## Troubleshooting

### Troubleshooting datly build problems
There is possibility to get issue when building datly

```shell
datly build -p=~/myproject -r=standalone -d=~/myproject/bin -o=darwin -a=amd64
```
A possible cause of this issue is interrupting the building process e.g. by the killing of process.

It produces output like this:
```shell
[INFO] Build time: 0001-01-01 00:00:00 +0000 UTC
matched mainPath: cmd/datly/ cmd/datly/datly.go
2023/06/02 15:53:36 couldn't generate module due to the: exit status 1 at: /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/1685714014424373/plugin/cmd/datly
        stdin: /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go/go1.17.1/go/bin/go build -trimpath -ldflags="-X main.BuildTimeInS=`date +%s`" -o /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/1685714014424373/datly
        stdount: package github.com/viant/datly/cmd/datly
        imports github.com/go-sql-driver/mysql
        imports errors: no Go files in /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go/go1.17.1/go/src/errors
package github.com/viant/datly/cmd/datly
        imports fmt: no Go files in /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go/go1.17.1/go/src/fmt
package github.com/viant/datly/cmd/datly
        imports github.com/go-sql-driver/mysql
        imports crypto/tls
        imports crypto/x509
        imports crypto/x509/internal/macos
        imports internal/abi: no Go files in /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go/go1.17.1/go/src/internal/abi
...
**shortened log due to dozens of packages and imports**
...
package github.com/viant/datly/cmd/datly
        imports github.com/google/gops/agent
        imports golang.org/x/sys/unix
        imports unsafe: no Go files in /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go/go1.17.1/go/src/unsafe
,
        env: [GOROOT=/var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go/go1.17.1/go HOME=/var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/home PATH=/usr/bin:/usr/local/bin:/bin:/sbin:/usr/sbin GOPRIVATE=github.com/michael/mymodule/*]
```

Try to detect in log folder similar to this:  
**/var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go**

Remove it with all files inside
```shell
rm -rf /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go
```

Now you can try to build datly again.

```shell
datly plugin -p=~/myproject -r=repo/dev -o=darwin -a=amd64
```

