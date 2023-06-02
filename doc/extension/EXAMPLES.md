# How-to Guides
## General prerequisites
### Datly (TODO which version)

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


### 1.7 Initialise datly rule repository
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

**Tip**
You can init repository with more than 1 connector i.e.:
```shell
datly init -p=~/myproject \
-c='sakiladb01|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila01?parseTime=true' \
-c='sakiladb02|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila02?parseTime=true' \
-r=repo/dev
```
+ connections.yaml content:
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
If you want use different than default connector you have to add $DB[<connector_name>] param prefix before db source
inside dsql file i.e. in Actor.sql:
```sql
#set($_ = $curActor<[]*Actor>(data_view/curActor) /* ? 
  select * from $DB[sakiladb02].actor
  WHERE $criteria.In("actor_id", $ActorActorId.Values) 
  */
)
```

### 1.8 Generate repo rules from dsql
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

TODO
if you want user more connections
describe how to play with more than 1 connection

if you want use different repo use -f option like here:
datly init \
-p=~/myproject \
-c='ci_ads|mysql|root:dev@tcp(127.0.0.1:3306)/ci_ads?parseTime=true' \
-r=repo/dev/
-f=ws


+ connections.yaml
+ Actor.sql
+ curActor.sql
+ Actor.yaml
+ config.json


### 1.10 Build standalone app 
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

### 1.11 Run app
```shell
~/myproject/bin/datly run -c=~/myproject/repo/p_ssW0rd/Datly/config.json
```

+single insert
+single update
+multi insert
+multi update