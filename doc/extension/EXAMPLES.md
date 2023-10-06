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

## 1 Basic patch service for actor table - standalone app in custom mode (using datly handler)

### 1.1 Generate project
   ```shell
   datly initExt -p=~/myproject -n=mymodule
   ```
The following project structure get generated

```bash
myproject
├── .build
│   ├── datly
│   └── ext
├── dql
└── pkg
    ├── bootstrap
    ├── checksum
    ├── dependency
    ├── go.mod
    ├── go.sum
    └── xinit.go
```

### 1.2 Create folder for actor's resources
   ```shell
   mkdir -p ~/myproject/dql/actor/init
   ```

### 1.3 Create actor's init sql file
   ```shell
   touch ~/myproject/dql/actor/init/actor_patch_init.sql
   ```
```shell
myproject
...
├── dql
│   └── actor
│       └── init
│           └── actor_patch_init.sql
...
```

### 1.4 Add content to actor_patch_init.sql

e.g. for vi users
```shell
vi ~/myproject/dql/actor/init/actor_patch_init.sql
```

```sql
/* { "URI":"actor"} */
SELECT  Actor.* /* {"Cardinality":"Many", "Field":"Entity" } */
FROM (select * from actor) Actor
```

### 1.5 Generate go files and dql file for patch operations ~~and JSON entity~~
```shell
datly gen \
-o=patch \
-c='sakiladb|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila?parseTime=true' \
-s=dql/actor/init/actor_patch_init.sql \
-g=actor \
-p=~/myproject \
-l=go
```

The following folders and files get generated or updated
```shell
myproject
...
├── dql
│   └── actor
│       └── Actor_patch.sql
└── pkg
    ├── actor
    │   ├── actor.go
    │   ├── handler.go
    │   ├── index.go
    │   └── state.go
    ├── checksum
    │   └── init.go
    ├── dependency
    │   └── init.go
    ├── plugin
    │   └── main.go
...
```

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
└── repo
    └── dev
        └── Datly
            ├── assets
            ├── config.json
            ├── dependencies
            │   └── connections.yaml
            └── routes

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

First connector in connections.yaml is a default connector for all dql files.
You can use connector different from default.

**Warning!**  
***Choosing connector option is currently implemented only for reader service.***  
***You can't use it for insert/update/delete operations because they use executor service.***


Add $DB[<connector_name>] param prefix before db source inside dql file i.e. in ActorReader.sql:
```sql
/* { "URI":"reader/actor"} */
SELECT  actor.*
FROM (select * from $DB[sakiladb02].actor) actor
```

### 1.7 Generate repo rules from dql
```shell
datly translate -s=dql/actor/Actor_patch.sql \
-p=~/myproject \
-c='sakiladb|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila?parseTime=true' \
-r=repo/dev
```
The following folders and files get generated
```shell
 myproject
  ...
  └── repo
    └── dev
        └── Datly
            └── routes
                └── dev
                    ├── Actor_patch
                    │   ├── Actor_patch.sql
                    │   └── CurActor.sql
                    └── Actor_patch.yaml
  ...
```

### 1.8 Build standalone app
linux
```shell
datly build -p=~/myproject -r=standalone -d=~/myproject/bin -o=linux -a=amd64 &&\
chmod u+x ~/myproject/bin/datly
```
macos
```shell
datly build -p=~/myproject -r=standalone -d=~/myproject/bin -o=darwin -a=amd64 &&\
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

[INFO] initialised datly: 11.200609ms
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
- **Tip**: We have to pass 4 fields (so far) in the request body because they are all required by db.
- ~~You can simple use~~ ~~/myproject/dql/actor/EntityPost.json file as a template. It contains just required fields.~~

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

### 1.12 Add initialiseForInsert method, which will uppercase actor's name
+ add ~/myproject/pkg/actor/init.go file with content:
```shell
vi ~/myproject/pkg/actor/init.go
```
```go
package actor

import (
	"strings"
	"time"
)

func (a *Actor) initialiseForInsert() {
	a.FirstName = strings.ToUpper(a.FirstName)
	if !a.Has.LastUpdate {
		a.LastUpdate = time.Now()
		a.Has.LastUpdate = true
	}
}
```

+ add method invocation in handler.go file
```go
if curActorByActorId.Has(recActor.ActorId) == true {
    if err = sql.Update("actor", recActor); err != nil {
        return nil, err
    }
} else {
    recActor.initialiseForInsert()
    if err = sql.Insert("actor", recActor); err != nil {
        return nil, err
    }
}
```


#### Rebuild and restart
- press Ctrl + C in terminal you run datly (kill datly server)
- build and run datly

linux
```shell
datly build -p=~/myproject -r=standalone -d=~/myproject/bin -o=linux -a=amd64 &&\
chmod u+x ~/myproject/bin/datly &&\
~/myproject/bin/datly run -c=~/myproject/repo/dev/Datly/config.json
```
macos
```shell
datly build -p=~/myproject -r=standalone -d=~/myproject/bin -o=darwin -a=amd64 &&\
chmod u+x ~/myproject/bin/datly &&\
~/myproject/bin/datly run -c=~/myproject/repo/dev/Datly/config.json
```

- or [Generate plugin](#generate-plugin)



+ Check if initialiseForInsert works

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

### 1.13 Add initialiseForUpdate method that will set lastUpdate and lastName fields
+ add method into ~/myproject/pkg/actor/init.go file:
```go
func (a *Actor) initialiseForUpdate(cur *Actor) {
	firstNameUpper := false

	if a.Has.LastName { //set last name uppercase if a first name in uppercase
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
}
```

+ add initialiseForUpdate invocation inside file ~/myproject/dql/actor/Actor_patch.sql

```go
if curActorByActorId.Has(recActor.ActorId) == true {
    recActor.initialiseForUpdate(curActorByActorId[recActor.ActorId])
    if err = sql.Update("actor", recActor); err != nil {
        return nil, err
    }
} else {
    recActor.initialiseForInsert()
    if err = sql.Insert("actor", recActor); err != nil {
        return nil, err
    }
}
```
- [Rebuild and restart](#rebuild-and-restart) or [Generate plugin](#generate-plugin)

+ Check if initialiseForUpdate works

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

### 1.14 Refactor init functions

- Change ~/myproject/pkg/actor/init.go file:
  - Wrap initialiseForInsert and initialiseForUpdate methods into new Init method.
```go
func (a *Actor) Init(cur *Actor) {
	isInsert := cur == nil
	if isInsert {
		a.initialiseForInsert()
	} else {
		a.initialiseForUpdate(cur)
	}
}
```
+ adjust file ~/myproject/pkg/actor/handler.go (two cases):

  + **mysql and sequencer case (our case) when the db table has required fields (more than id field)**  
    This case requires running initialization before using a sequencer.
```go
func (h *Handler) Exec(ctx context.Context, sess handler.Session) (interface{}, error) {
	state := &State{}
	if err := sess.Stater().Into(ctx, state); err != nil {
		return nil, err
	}

	sql, err := sess.Db()
	if err != nil {
		return nil, err
	}
	sequencer := sql

	actor := state.Actor
	curActor := state.CurActor

	curActorByActorId := ActorSlice(curActor).IndexByActorId()

	for _, recActor := range actor {
		recActor.Init(curActorByActorId[recActor.ActorId])
	}

	if err = sequencer.Allocate(ctx, "actor", actor, "ActorId"); err != nil {
		return nil, err
	}

	for _, recActor := range actor {
		if curActorByActorId.Has(recActor.ActorId) == true {
			if err = sql.Update("actor", recActor); err != nil {
				return nil, err
			}
		} else {
			if err = sql.Insert("actor", recActor); err != nil {
				return nil, err
			}
		}
	}

	return state.Actor, nil
}
```
  + general case
```go
...
for _, recActor := range actor {
    recActor.Init(curActorByActorId[recActor.ActorId])
    
    if curActorByActorId.Has(recActor.ActorId) == true {
        if err = sql.Update("actor", recActor); err != nil {
            return nil, err
        }
    } else {
        if err = sql.Insert("actor", recActor); err != nil {
            return nil, err
        }
    }
}
...
```

- [Rebuild and restart](#rebuild-and-restart) or [Generate plugin](#generate-plugin)

### 1.15 Validation using tags, custom validation function and custom result handling
+ Datly allows validating entities using tags.  
  Available tags:
  - sqlx
  - validate


+ Add validate tags in Actor struct in file ~/myproject/pkg/actor/actor.go
```go
type Actor struct {
	ActorId    int       `sqlx:"name=actor_id,autoincrement,primaryKey"`
	FirstName  string    `sqlx:"name=first_name" validate:"required,le(45)"`
	LastName   string    `sqlx:"name=last_name,unique,table=actor" validate:"required,le(45)"`
	LastUpdate time.Time `sqlx:"name=last_update" validate:"required"`
	Has        *ActorHas `setMarker:"true" typeName:"ActorHas" json:"-"  sqlx:"-" `
}
```
  - validate: "le(45)" allows for strings with lengths shorter or equal to 45
  - validate: "required" doesn't allow for nil value
  - sqlx: "unique,table=actor" checks in table actor if a value is unique
  -
- Read more about validation tags
  - [sqlx](https://github.com/viant/sqlx#validator-service)
  - [govalidator](https://github.com/viant/govalidator#usage)


+ create folder ~/myproject/pkg/shared
+ create file ~/myproject/pkg/shared/vresult.go
```go
package shared

import (
	"bytes"
	"fmt"
	hvalidator "github.com/viant/xdatly/handler/validator"
	"strings"
)

type ValidationResult struct {
	Validations []*hvalidator.Validation
	Failed      bool
}

func NewValidationResult(size int) *ValidationResult {
	r := &ValidationResult{}
	r.Validations = make([]*hvalidator.Validation, size)
	return r
}

func (r *ValidationResult) validationToString(position int, validation *hvalidator.Validation) string {

	if validation == nil || len(validation.Violations) == 0 {
		return ""
	}
	msg := strings.Builder{}
	msg.WriteString(fmt.Sprintf("Failed validation for Entity[%d]: ", position))
	for i, v := range validation.Violations {
		if i > 0 {
			msg.WriteString(",\n")
		}
		msg.WriteString(v.Location)
		msg.WriteString(" (")
		msg.WriteString(v.Check)
		msg.WriteString(")")
		msg.WriteString(" - ")
		msg.WriteString(v.Message)
	}
	return msg.String()
}

func (r *ValidationResult) Error() string {
	var buffer bytes.Buffer
	if r.Failed {
		for i, v := range r.Validations {
			if i > 0 {
				buffer.WriteString(",\n")
			}
			buffer.WriteString(r.validationToString(i, v))
		}
	}
	return buffer.String()
}

func (r *ValidationResult) ErrorStatusCode() int {
	return 422 //Unprocessable Entity
}

func (r *ValidationResult) ErrorMessage() string {
	return r.Error()
}
```

+ create file ~/myproject/pkg/actor/validate.go
```go
package actor

import (
	"context"
	"fmt"
	"github.com/michael/mymodule/shared"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/sqlx"
	hvalidator "github.com/viant/xdatly/handler/validator"
	"strings"
)

type ValidationResult struct {
	Validations []*hvalidator.Validation
	Failed      bool
}

func (r *ValidationResult) Init(size int) {
	r.Validations = make([]*hvalidator.Validation, size)
}

func validateAll(actor []*Actor, session handler.Session) *shared.ValidationResult {
	validationRes := shared.NewValidationResult(len(actor))
	var err error
	for i, recActor := range actor {
		validationRes.Validations[i], err = recActor.validate(recActor, session)
		if validationRes.Validations[i].Failed {
			validationRes.Failed = true
		}
		if err != nil {
			return validationRes
		}
	}
	return validationRes
}

func (a *Actor) validate(cur *Actor, session handler.Session) (*hvalidator.Validation, error) {

	aValidation, err := a.validateWithTags(session)
	if err != nil {
		return aValidation, err
	}

	isInsert := cur == nil
	if isInsert {
		a.validateForInsert(aValidation)
	} else {
		a.validateForUpdate(aValidation, cur)
	}
	return aValidation, nil
}

func (a *Actor) validateWithTags(session handler.Session) (*hvalidator.Validation, error) {
	aValidation := &hvalidator.Validation{}
	service, err := session.Db(sqlx.WithConnector("sakiladb"))
	if err != nil {
		aValidation.AddViolation("/", "", "", "error", fmt.Sprintf("VALIDATION_ERROR: %s", err.Error()))
		return aValidation, err
	}

	db, err := service.Db(context.TODO())
	if err != nil {
		aValidation.AddViolation("/", "", "", "error", fmt.Sprintf("VALIDATION_ERROR: %s", err.Error()))
		return aValidation, err
	}

	sValidator := session.Validator()
	if sValidator == nil {
		aValidation.AddViolation("/", "", "", "error", fmt.Sprintf("VALIDATION_ERROR: %s", "session.Validator() returned nil"))
		return aValidation, err
	}

	aValidation, err = sValidator.Validate(context.Background(), a,
		hvalidator.WithShallow(true),
		hvalidator.WithSetMarker(true), // TODO add hvalidator.WithUnique()
		hvalidator.WithDB(db),
	)
	if err != nil {
		aValidation.AddViolation("/", "", "", "error", fmt.Sprintf("VALIDATION_ERROR: %s", err.Error()))
		return aValidation, err
	}

	return aValidation, nil
}

func (a *Actor) validateForInsert(validation *hvalidator.Validation) {
	if a.Has.FirstName && a.Has.LastName {
		a.validateNames(validation, a.FirstName, a.LastName)
	}
}

func (a *Actor) validateForUpdate(validation *hvalidator.Validation, cur *Actor) {
	firstName := cur.FirstName
	lastName := cur.LastName

	if a.Has.FirstName {
		firstName = a.FirstName
	}

	if a.Has.LastName {
		lastName = a.LastName
	}

	a.validateNames(validation, firstName, lastName)
}

func (a *Actor) validateNames(validation *hvalidator.Validation, firstName string, lastName string) {
	if len(firstName) > 0 && len(a.LastName) > 0 {
		if strings.ToUpper(string([]rune(firstName)[0])) == strings.ToUpper(string([]rune(lastName)[0])) {
			validation.AddViolation("[FirstName, LastName]",
				"[FirstName, LastName]",
				fmt.Sprintf("%s %s", firstName, lastName),
				"theSameFirstLetter",
				fmt.Sprintf("First name and last name can't start with the same letter %s %s", firstName, lastName))
		}
	}
}
```

+ add Validate invocation inside file ~/myproject/pkg/actor/actor.go
```go
...
	for _, recActor := range actor {
		recActor.Init(curActorByActorId[recActor.ActorId])
	}

	validationRes := validateAll(actor, sess)
	if validationRes.Failed {
		return nil, validationRes
	}

	if err = sequencer.Allocate(ctx, "actor", actor, "ActorId"); err != nil {
		return nil, err
	}
...
```
- [Rebuild and restart](#rebuild-and-restart) or [Generate plugin](#generate-plugin)

+ Check if validation works
```http request
PATCH /v1/api/dev/actor HTTP/1.1
Host: 127.0.0.1:8080
Content-Type: application/json
```
```json
{
  "Entity": [
    {
      "firstName": "Mike",
      "lastName": "Wazalsky0123456789012345678901234567890123456789"
    },
    {
      "firstName": null,
      "lastName": "Wazalsky"
    },
    {
      "firstName": "Willy",
      "lastName": "Wazalsky"
    }
  ]
}
```
The response should be like:
```code
{
  "Status":"error",
  "Message":"Failed validation for Entity[0]: 
                LastName (le) - check 'le' failed on field LastName,
             Failed validation for Entity[1]: 
                FirstName (required) - check 'required' failed on field FirstName,
                LastName (unique) - value 'Wazalsky' is not unique"
             Failed validation for Entity[2]: 
                LastName (unique) - value 'Wazalsky' is not unique,
                [FirstName, LastName] (theSameFirstLetter) - First name and last name can't start with the same letter WILLY Wazalsky"
}
```

- [Generate plugin](#generate-plugin)
- [Generate repo rules for Actor_patch.sql](#17-generate-repo-rules-from-dql)





```sql
CREATE TABLE `DIFF_JN` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `DIFF` longtext,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
```

## 2 Basic patch service for actor table - standalone app in custom mode (with velty template)

### 2.1 Generate project
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
  | - dql
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
  |    | - go.sum
  |    | - xinit.go
```

### 2.2 Create folder for actor's resources
   ```shell
   mkdir -p ~/myproject/dql/actor/init
   ```

### 2.3 Create actor's init sql file
   ```shell
   touch ~/myproject/dql/actor/init/actor_patch_init.sql
   ```
```shell
 myproject
  ...
  | - dql 
  |    | - actor
  |         | - init
  |              | - actor_patch_init.sql
  ...
```

### 2.4 Add content to actor_patch_init.sql

e.g. for vi users
```shell
vi ~/myproject/dql/actor/init/actor_patch_init.sql
```

```sql
/* { "URI":"actor"} */
SELECT  Actor.* /* {"Cardinality":"Many", "Field":"Entity" } */
FROM (select * from actor) Actor
```

### 2.5 Generate go files and dql file for patch operations and JSON entity
```shell
datly gen \
-o=patch \
-c='sakiladb|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila?parseTime=true' \
-s=dql/actor/init/actor_patch_init.sql \
-g=actor \
-p=~/myproject
```

The following folders and files get generated or updated
```shell
 myproject
  ...
  | - dql 
  |    | - actor
  |         | - Actor_patch.sql
  |         | - EntityPost.json
  |      
  | - pkg 
  |    | - actor
  |    |    | - actor.go 
  |    |
  |    | - dependency
  |         | - init.go
  ...
```
+ Actor_patch.sql - patch logic written in dql
+ ~~EntityPost.json - request body template just with required fields`~~
+ actor.go - all needed go structs
+ init.go - updated imports

### 2.6 Initialise datly rule repository
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

First connector in connections.yaml is a default connector for all dql files.
You can use connector different from default.

**Warning!**  
***Choosing connector option is currently implemented only for reader service.***  
***You can't use it for insert/update/delete operations because they use executor service.***


Add $DB[<connector_name>] param prefix before db source inside dql file i.e. in ActorReader.sql:
```sql
/* { "URI":"reader/actor"} */
SELECT  actor.*
FROM (select * from $DB[sakiladb02].actor) actor
```

### 2.7 Generate repo rules from dql
```shell
datly translate -s=dql/actor/Actor_patch.sql \
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
  |                        |    | - Actor_patch.sql
  |                        |    | - curActor_patch.sql
  |                        |
  |                        | - Actor.yaml
  ...
```

### 2.8 Build standalone app
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

### 2.9 Run app
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

[INFO] initialised datly: 11.200609ms
starting endpoint: 8080
```

### 2.10 Add new actor by sending request using e.g. Postman
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

### 2.11 Update actor's name inserted before
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
You can simple use ~/myproject/dql/actor/EntityPost.json file as a template. It contains just required fields.

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

### 2.12 Add InitialiseForInsert method, which will uppercase actor's name
+ add ~/myproject/pkg/actor/init.go file with content:
```go
package actor

import "strings"

func (a *Actor) initialiseForInsert() bool {
	a.FirstName = strings.ToUpper(a.FirstName)
	if !a.Has.LastUpdate {
		a.LastUpdate = time.Now()
		a.Has.LastUpdate = true
	}
	return true
}
```
#### Generate plugin  

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
+ add InitialiseForInsert invocation inside file ~/myproject/dql/actor/Actor_patch.sql

```code
#if(($curActorByActorId.HasKey($recActor.ActorId) == true))
  $sql.Update($recActor, "actor");
#else
  #set($inited = $recActor.InitialiseForInsert())
  $sql.Insert($recActor, "actor");
#end
```

- [Generate repo rules for Actor_patch.sql](#17-generate-repo-rules-from-dql)

**Tip:**  
You don't have to build and deploy app after changing rules or code in go.
Generated plugins and rules are automatically reloaded by the app on runtime.  

**Warning:**
- You can use only public method's invocations inside DSQL files (in this case Actor_patch.sql)
- Method has to return value.
- Can't use nil as function argument value without variable (inside DSQL file) 

### 2.13 Check if InitialiseForInsert works

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

### 2.14 Add InitialiseForUpdate method that will set lastUpdate and lastName fields
+ add method into ~/myproject/pkg/actor/init.go file:
```go
func (a *Actor) initialiseForUpdate(cur *Actor) bool {
	firstNameUpper := false

	if a.Has.LastName { //set last name uppercase if a first name in uppercase
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
- [Generate plugin](#generate-plugin)

+ add InitialiseForUpdate invocation inside file ~/myproject/dql/actor/Actor_patch.sql

```code
#if(($curActorByActorId.HasKey($recActor.ActorId) == true))
  #set($inited = $recActor.InitialiseForUpdate($curActorByActorId[$recActor.ActorId]))
  $sql.Update($recActor, "actor");
#else
  #set($inited = $recActor.InitialiseForInsert())
  $sql.Insert($recActor, "actor");
#end
```

- [Generate repo rules for Actor_patch.sql](#17-generate-repo-rules-from-dql)


### 2.15 Check if InitialiseForUpdate works

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


### 2.17 Refactor init functions

- Change ~/myproject/pkg/actor/init.go file:
  - Change InitialiseForInsert and InitialiseForUpdate methods to private ones.
  - Wrap them into new Init method.
```go
func (a *Actor) Init(cur *Actor) bool {
	isInsert := cur == nil
	if isInsert {
		return a.initialiseForInsert()
	} else {
		return a.initialiseForUpdate(cur)
	}
}
```

+ adjust file ~/myproject/dql/actor/Actor_patch.sql (two cases):  

  + **mysql and sequencer case (our case) when the db table has required fields (more than id field)**  
    This case requires running initialization before using a sequencer.
  ```code
  /* {"URI":"actor","Method":"PATCH","ResponseBody":{"From":"Actor"}} */
  
  import (
      "actor.Actor"
      "actor.Entity"
  )
  
  #set($_ = $Actor<[]*Actor>(body/Entity))
  #set($_ = $ActorActorId<?>(param/Actor) /*
     ? SELECT ARRAY_AGG(ActorId) AS Values FROM  `/` LIMIT 1
     */
  )
  
  #set($_ = $curActor<[]*Actor>(data_view/curActor) /* ?
    select * from actor
    WHERE $criteria.In("actor_id", $ActorActorId.Values)
    */
  )
  
  #set($curActorByActorId = $curActor.IndexBy("ActorId"))
  
  #foreach($recActor in $Unsafe.Actor)
      #if($recActor)
          #set($inited = $recActor.Init($curActorByActorId[$recActor.ActorId]))
          #if($inited ==  false)
              #set($initError = "init error")
              $response.StatusCode(401)
              $response.Failf("%v",$initError)
          #end
      #end
  #end
  
  $sequencer.Allocate("actor", $Actor, "ActorId")
  
  #foreach($recActor in $Unsafe.Actor)
      #if($recActor)
          #if(($curActorByActorId.HasKey($recActor.ActorId) == true))
            $sql.Update($recActor, "actor");
          #else
            $sql.Insert($recActor, "actor");
          #end
      #end
  #end
  ```
  + general case
  ```code
  ...
  #foreach($recActor in $Unsafe.Actor)
      #if($recActor)
          #set($inited = $recActor.Init($curActorByActorId[$recActor.ActorId]))
  
          #if(($curActorByActorId.HasKey($recActor.ActorId) == true))
            $sql.Update($recActor, "actor");
          #else
            $sql.Insert($recActor, "actor");
          #end
      #end
  #end
  ```
- [Generate plugin](#generate-plugin)
- [Generate repo rules for Actor_patch.sql](#17-generate-repo-rules-from-dql)

### 2.18 Default struct's validation with tags
+ Datly allows validating entities using tags.  
Available tags:  
  - ~~sqlx~~ (temporarily under reconstruction) // TODO
  - validate


+ Add validate tags in Actor struct in file ~/myproject/pkg/actor/actor.go
  ```go
  type Actor struct {
      ActorId    int       `sqlx:"name=actor_id,autoincrement,primaryKey,required"`
      FirstName  string    `sqlx:"name=first_name,required" validate:"ge(2),le(15)"`
      LastName   string    `sqlx:"name=last_name,unique,table=actor,required"  validate:"ge(2),le(15)"`
      LastUpdate time.Time `sqlx:"name=last_update,required"`
      Has        *ActorHas `setMarker:"true" typeName:"ActorHas" json:"-"  sqlx:"-" `
  }
  ```
  - validate:"ge(2),le(15)" allows for strings with length between 2 and 15.
  - ~~sqlx~~:"required" allows not nil value
  - ~~sqlx~~:"unique,table=actor" checks in table actor if a value is unique
  - 
- Check more about validation tags
  - [~~sqlx~~](https://github.com/viant/sqlx#validator-service)
  - [govalidator](https://github.com/viant/govalidator#usage)
  
+ Check if default validation works
```http request
PATCH /v1/api/dev/actor HTTP/1.1
Host: 127.0.0.1:8080
Content-Type: application/json
```
```json
{
    "Entity": [
        {
            "firstName": "M",
            "lastName": "Wazalsky0123456789"
        }
    ]
}
```
The response should be like:
```json
{
    "Status": "error",
    "Message": "Failed validation for Entity[0].FirstName(ge),Entity[0].LastName(le)",
    "Errors": [
        {
            "View": "Actor",
            "Param": "ActorActorId",
            "Message": "Failed validation for Entity[0].FirstName(ge),Entity[0].LastName(le)",
            "Object": [
                {
                    "Location": "Entity[0].FirstName",
                    "Field": "FirstName",
                    "Value": "M",
                    "Message": "check 'ge' failed on field FirstName",
                    "Check": "ge"
                },
                {
                    "Location": "Entity[0].LastName",
                    "Field": "LastName",
                    "Value": "Wazalsky0123456789",
                    "Message": "check 'le' failed on field LastName",
                    "Check": "le"
                }
            ]
        }
    ]
}
```

////////////
### 2.19 Add custom validation
+ **modify file ~/myproject/pkg/actor/validate.go**
```go
package actor

import (
	"fmt"
	"github.com/michael/mymodule/shared"
	"github.com/viant/govalidator"
	"strings"
)

func (a *Actor) Validate(cur *Actor) *shared.Validation {
	info := shared.NewValidationInfo()
	info.Validate(a, govalidator.WithShallow(true), govalidator.WithSetMarker())
	defer info.UpdateStatus()

	isInsert := cur == nil
	if isInsert {
		a.validateForInsert(info)
	} else {
		a.validateForUpdate(info, cur)
	}
	return info
}

func (a *Actor) validateForInsert(info *shared.Validation) {
	if a.Has.FirstName && a.Has.LastName {
		a.validateNames(info, a.FirstName, a.LastName)
	}
}

func (a *Actor) validateForUpdate(info *shared.Validation, cur *Actor) {
	firstName := cur.FirstName
	lastName := cur.LastName

	if a.Has.FirstName {
		firstName = cur.FirstName
	}

	if a.Has.LastName {
		lastName = a.LastName
	}

	a.validateNames(info, firstName, lastName)
}

func (a *Actor) validateNames(info *shared.Validation, firstName string, lastName string) {
	if len(firstName) > 0 && len(a.LastName) > 0 {
		if strings.ToUpper(string([]rune(firstName)[0])) == strings.ToUpper(string([]rune(lastName)[0])) {
			info.Validation.AddViolation("[FirstName, LastName]", fmt.Sprintf("%s %s", firstName, lastName), "theSameFirstLetter",
				fmt.Sprintf("First name and last name can't start with the same letter %s %s", firstName, lastName))
		}
	}
}
```

+ check if exists Validate invocation inside file ~/myproject/dql/actor/Actor_patch.sql
```code
#foreach($recActor in $Unsafe.Actor)
    #if($recActor)
        #set($inited = $recActor.Init($curActorByActorId[$recActor.ActorId]))

        #set($info = $recActor.Validate($curActorByActorId[$recActor.ActorId]))
        #if($info.HasError ==  true)
            $response.StatusCode(401)
            $response.Failf("%v",$info.Error)
        #end

        #if(($curActorByActorId.HasKey($recActor.ActorId) == true))
          $sql.Update($recActor, "actor");
        #else
          $sql.Insert($recActor, "actor");
        #end
    #end
#end
```

- [Generate plugin](#generate-plugin)
- [Generate repo rules for Actor_patch.sql](#17-generate-repo-rules-from-dql)
////////////
+ create folder ~/myproject/pkg/shared
+ create file ~/myproject/pkg/shared/message.go
```go
package shared

const (
	MessageLevelInfo = iota
	MessageLevelWarning
	MessageLevelError
)

type Message struct {
	Level   int
	Code    string
	Message string
}

type Messages struct {
	Messages []*Message
	Error    string
	HasError bool
}

func (m *Messages) AddInfo(code, message string) {
	m.Messages = append(m.Messages, &Message{Message: message, Code: code, Level: MessageLevelInfo})
}

func (m *Messages) AddWarning(code, message string) {
	m.Messages = append(m.Messages, &Message{Message: message, Code: code, Level: MessageLevelWarning})

}

func (m *Messages) AddError(code, message string) {
	m.Messages = append(m.Messages, &Message{Message: message, Code: code, Level: MessageLevelError})
	m.HasError = true
	m.Error = message
}
```

+ create file ~/myproject/pkg/shared/validation.go
```go
package shared

import (
	"context"
	"github.com/viant/govalidator"
)

var validator = govalidator.New()

//Validation represents validation info
type Validation struct {
	Validation govalidator.Validation
	Messages
}

func (v *Validation) UpdateStatus() {
	if v.Validation.Failed {
		v.Messages.AddError("VALIDATION_ERROR", v.Validation.String())
	}
}

func (v *Validation) FloatPairRequired(first, second *float64, location, message string) {
	if first == nil && second == nil {
		return
	}
	if second == nil {
		v.Validation.AddViolation(location, nil, "pairRequired", message)
	}
}

func (v *Validation) Validate(any interface{}, options ...govalidator.Option) bool {
	validation, err := validator.Validate(context.Background(), any, options...)
	if err != nil {
		v.Messages.AddError("VALIDATION_ERROR", err.Error())
		return false
	}

	if validation != nil {
		v.Validation.Violations = append(v.Validation.Violations, validation.Violations...)
		if validation.Failed {
			v.Validation.Failed = validation.Failed
		}
	}
	return v.Validation.Failed
}

func NewValidationInfo() *Validation {
	return &Validation{Validation: govalidator.Validation{}}
}
```

+ add required module (version can be different) in ~/myproject/pkg/go.mod
```text
github.com/viant/govalidator v0.2.1
```

+ **add file ~/myproject/pkg/actor/validate.go**
```go
package actor

import (
	"fmt"
	"github.com/michael/mymodule/shared"
	"github.com/viant/govalidator"
	"strings"
)

func (a *Actor) Validate(cur *Actor) *shared.Validation {
	info := shared.NewValidationInfo()
	defer info.UpdateStatus()

    info.Validate(a, govalidator.WithShallow(true), govalidator.WithSetMarker())
	
	return info
}
```
+ add Validate invocation inside file ~/myproject/dql/actor/Actor_patch.sql

```code
#foreach($recActor in $Unsafe.Actor)
    #if($recActor)
        #set($inited = $recActor.Init($curActorByActorId[$recActor.ActorId]))

        #set($info = $recActor.Validate($curActorByActorId[$recActor.ActorId]))
        #if($info.HasError ==  true)
            $response.StatusCode(401)
            $response.Failf("%v",$info.Error)
        #end

        #if(($curActorByActorId.HasKey($recActor.ActorId) == true))
          $sql.Update($recActor, "actor");
        #else
          $sql.Insert($recActor, "actor");
        #end
    #end
#end
```

+ **add validate tags in Actor struct in file ~/myproject/pkg/actor/actor.go**
```go
type Actor struct {
	ActorId    int       `sqlx:"name=actor_id,autoincrement,primaryKey,required"`
	FirstName  string    `sqlx:"name=first_name,required" validate:"ge(2),le(15)"`
	LastName   string    `sqlx:"name=last_name,unique,table=actor,required"  validate:"ge(2),le(15)"`
	LastUpdate time.Time `sqlx:"name=last_update,required"`
	Has        *ActorHas `setMarker:"true" typeName:"ActorHas" json:"-"  sqlx:"-" `
}
```
- We can use tags for struct validation from sqlx and govalidator package
  - Tag: validate:"ge(2),le(15)" allows for strings with length between 2 and 15.
  - Tag: sqlx:"required" allows not nil value
  - Tag: sqlx:"unique,table=actor" checks in table actor if a value is unique

  
- Check for more tags
  - [sqlx](https://github.com/viant/sqlx#validator-service)
  - [govalidator](https://github.com/viant/govalidator#usage)


- [Generate plugin](#generate-plugin)
- [Generate repo rules for Actor_patch.sql](#17-generate-repo-rules-from-dql)

+ **If you insert/update (patch) an actor with a first name which length is less than 3 
then you get a validation error like this**
```text
{
    "Status": "error",
    "Message": "Failed validation for FirstName(gt)"
}
```

### 2.19 Add custom validation
+ **modify file ~/myproject/pkg/actor/validate.go**
```go
package actor

import (
	"fmt"
	"github.com/michael/mymodule/shared"
	"github.com/viant/govalidator"
	"strings"
)

func (a *Actor) Validate(cur *Actor) *shared.Validation {
	info := shared.NewValidationInfo()
	info.Validate(a, govalidator.WithShallow(true), govalidator.WithSetMarker())
	defer info.UpdateStatus()

	isInsert := cur == nil
	if isInsert {
		a.validateForInsert(info)
	} else {
		a.validateForUpdate(info, cur)
	}
	return info
}

func (a *Actor) validateForInsert(info *shared.Validation) {
	if a.Has.FirstName && a.Has.LastName {
		a.validateNames(info, a.FirstName, a.LastName)
	}
}

func (a *Actor) validateForUpdate(info *shared.Validation, cur *Actor) {
	firstName := cur.FirstName
	lastName := cur.LastName

	if a.Has.FirstName {
		firstName = cur.FirstName
	}

	if a.Has.LastName {
		lastName = a.LastName
	}

	a.validateNames(info, firstName, lastName)
}

func (a *Actor) validateNames(info *shared.Validation, firstName string, lastName string) {
	if len(firstName) > 0 && len(a.LastName) > 0 {
		if strings.ToUpper(string([]rune(firstName)[0])) == strings.ToUpper(string([]rune(lastName)[0])) {
			info.Validation.AddViolation("[FirstName, LastName]", fmt.Sprintf("%s %s", firstName, lastName), "theSameFirstLetter",
				fmt.Sprintf("First name and last name can't start with the same letter %s %s", firstName, lastName))
		}
	}
}
```

+ check if exists Validate invocation inside file ~/myproject/dql/actor/Actor_patch.sql
```code
#foreach($recActor in $Unsafe.Actor)
    #if($recActor)
        #set($inited = $recActor.Init($curActorByActorId[$recActor.ActorId]))

        #set($info = $recActor.Validate($curActorByActorId[$recActor.ActorId]))
        #if($info.HasError ==  true)
            $response.StatusCode(401)
            $response.Failf("%v",$info.Error)
        #end

        #if(($curActorByActorId.HasKey($recActor.ActorId) == true))
          $sql.Update($recActor, "actor");
        #else
          $sql.Insert($recActor, "actor");
        #end
    #end
#end
```

- [Generate plugin](#generate-plugin)
- [Generate repo rules for Actor_patch.sql](#17-generate-repo-rules-from-dql)

+ **If you insert/update (patch) actor with a first name and last name beginning with the same char 
then you get a validation error like this:**
```text
{
    "Status": "error",
    "Message": "Failed validation for [FirstName, LastName](theSameFirstLetter)"
}
```

```sql
CREATE TABLE `DIFF_JN` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `DIFF` longtext,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
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
### Step 1
First, try to find in your log fragment like this:
```shell
env: [GOROOT=/var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go/go1.17.1/go HOME=/var/folder[...]
```

Next, try to delete subfolders from  
- GOROOT  
**/var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go/go1.17.1/go** 
  

- HOME  
**/var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/home** 

like figured below:
```shell
sudo rm -rf /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/go
sudo rm -rf /var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/home
```

Now you can try to build datly again.

```shell
datly plugin -p=~/myproject -r=repo/dev -o=darwin -a=amd64
```

### Step 2
- Check if **myproject/pkg/go.mod** uses modules with the same version like **myproject/.build/datly/go.mod**
- Check if you can run command **go build** without errors in folders
  - ~/myproject2/pkg
  - ~/myproject2/.build/datly/cmd/datly

Now you can try to build datly again.
```shell
datly plugin -p=~/myproject -r=repo/dev -o=darwin -a=amd64
```

### Troubleshooting datly load plugin
If you build a plugin that uses a module with a different version than datly inside your_project_dir/.build 
then this kind of error can occur when you run app or reload plugin:

- command:
```shell
 ~/myproject/bin/datly run -c=~/myproject/repo/dev/Datly/config.json
```

- error message:
```shell
[ERROR] error occured while reading plugin 
        plugin.Open("/var/folders/6z/v17fqdzs273b2qrf9jdkdq1m0000gn/T/20230609122553/main_1_17_1_darwin_amd64"): 
        plugin was built with a different version of package github.com/viant/govalidator
```

- In this case go.mod files had different version of github.com/viant/govalidator  

  - myproject/pkg/go.mod:
  ```text
  github.com/viant/govalidator v0.2.1
  ```
  
  - myproject/.build/datly/go.mod:
  ```text
  github.com/viant/govalidator v0.2.0
  ```

Because these versions weren't compatible I decided 
to update .build/datly to latest version that uses v0.2.1  

After deleting whole .build dir run command to recreate it:
```shell
datly initExt -p=~/myproject -n=mymodule   
```
Now you should be able to load the plugin.

### Troubleshooting starting compiled project - unterminated statements on the stack
command:
```shell
~/myproject/bin/datly run -c=~/myproject/repo/dev/Datly/config.json
```

error:
```
2023/06/22 20:30:38 failed to load routers due to the: unterminated statements on the stack: [0xc0002a89c0 0xc000ba82a0]
```

reason 1:  
Missing bracket in variable definition, in entity rule file (in this current case ~/myproject/dql/actor/Actor_patch.sql)
```
#set($result = $recActor.Validate($curActorByActorId[$recActor.ActorId], $session)
```
solution:
```
#set($result = $recActor.Validate($curActorByActorId[$recActor.ActorId], $session))
```

reason 2:
Wrong project/module names inside files (check if they have proper values): 
**.build/ext/init.go:**
import _ "github.com/<USER>/mymodule"

**.build/ext/go.mod:**
require github.com/<USER>/mymodule v0.0.0-00010101000000-000000000000
replace github.com/<USER>/mymodule => /Users/<USER>/myproject/pkg

**.build/datly/go.mod:**
github.com/<USER>/mymodule v0.0.0-00010101000000-000000000000 // indirect
replace github.com/<USER>/mymodule => /Users/<USER>/myproject/pkg
replace github.com/viant/xdatly/extension => /Users/<USER>/myproject/.build/ext

## 8 Debugging
### 8.1 More debug information on runtime
Set environment variable before running app to get more debug information.
```text
export DATLY_NOPANIC="1"
```
### 8.2 Using datly.go for debugging
+ Adjust main function in file ~/myproject/.build/datly/e2e/debug/datly.go
```go
func main() {
	os.Setenv("DATLY_NOPANIC", "0")

	// Adjust debug options
	os.Setenv("DATLY_DEBUG", "true")
	read.ShowSQL(true)
	update.ShowSQL(true)
	insert.ShowSQL(true)

	// Set path to your repo config
	configURL := filepath.Join("~/myproject/repo/dev/Datly/config.json")
	os.Args = []string{"", "-c=" + configURL}
	fmt.Printf("[INFO] Build time: %v\n", env.BuildTime.String())

	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()
	server, err := cmd.New(Version, os.Args[1:], &ConsoleWriter{})
	if err != nil {
		log.Fatal(err)
	}

	if server != nil {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err.Error())
		}
	}
}
```

+ Add breakpoint in handler.go file

```go
func (h *Handler) Exec(ctx context.Context, sess handler.Session) (interface{}, error) {
state := &State{} // set breakpoint at this line
...
}
```

+ Run debugger


### 8.3 Create unit test file on project level
+ add file ~/myproject/service_test.go
```go
package myproject2

import (
	"context"
	"fmt"
	"github.com/viant/datly"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/update"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/xdatly/types/custom/actor"

	"io"
	"log"
	"net/http"
	surl "net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestService_Patch(t *testing.T) {
	testCases := []struct {
		description string
		Email       string
		UserID      int
		method      string
		service     string
		viewName    string
		ruleURL     string
		rawURL      string
		pkg         string
		name        string
		rType       reflect.Type
		body        string
	}{
		{
			description: "actor",
			Email:       "dev@viantinc.com",
			UserID:      56453,
			method:      "patch",
			viewName:    "Actor",
			ruleURL:     "/Users/michael/myproject2/repo/dev/Datly/routes/dev/Actor.yaml",
			rawURL:      "http://127.0.0.1:8080/v1/api", // "http://127.0.0.1:8080/v1/api/dev",
			pkg:         "actor",
			name:        "Actor",
			rType:       reflect.TypeOf(actor.Actor{}),
			body: `{
    "Entity": [
        {
            "actorId": 0,
            "firstName":"AZ0123",
            "lastName": "Z12345"
        }
    ]
}`,
		},
	}

	for _, testCase := range testCases[0:1] {
		//	option.PresenceProvider
		os.Setenv("DATLY_DEBUG", "true")

		//Uncomment various additional debugging option and debugging and troubleshooting
		expand.SetPanicOnError(false)
		read.ShowSQL(true)
		update.ShowSQL(true)
		insert.ShowSQL(true)
		ctx := context.Background()
		service := datly.New(datly.NewConfig())
		viewName := testCase.viewName
		err := service.LoadRoute(ctx, testCase.ruleURL,
			view.NewPackagedType(testCase.pkg, testCase.name, testCase.rType),
		)
		//	p := velty.Planner{}
		if err != nil {
			log.Fatal(err)
		}
		err = service.Init(ctx)
		if err != nil {
			log.Fatal(err)
		}

		URL, _ := surl.Parse(testCase.rawURL)
		httpRequest := &http.Request{
			URL:    URL,
			Method: testCase.method,
			Body:   io.NopCloser(strings.NewReader(testCase.body)),
			Header: http.Header{},
		}

		token, err := service.JwtSigner.Create(time.Hour, &jwt.Claims{
			Email:  testCase.Email,
			UserID: testCase.UserID,
		})
		if err != nil {
			log.Fatal(err)
		}
		httpRequest.Header.Set("Authorization", "Bearer "+token)

		routeRes, _ := service.Routes()
		route := routeRes.Routes[0] //make sure you are using correct route
		err = service.Exec(ctx, viewName, datly.WithExecHttpRequest(ctx, route, httpRequest))
		//route.ResponseBody
		fmt.Println(route.ResponseBody.Query)
		//route.ResponseBody.Query
		if err != nil {
			log.Fatal(err)
		}
	}
}
```

+ add file ~/myproject/go.mod with equivalent content:
```mod
module github.com/viant/datly

go 1.20

require (
	github.com/aerospike/aerospike-client-go v4.5.2+incompatible
	github.com/aws/aws-lambda-go v1.31.0
	github.com/francoispqt/gojay v1.2.13
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator v9.31.0+incompatible
	github.com/go-sql-driver/mysql v1.7.0
	github.com/goccy/go-json v0.9.11
	github.com/golang-jwt/jwt/v4 v4.4.1
	github.com/google/gops v0.3.23
	github.com/google/uuid v1.3.0
	github.com/jessevdk/go-flags v1.5.0
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lib/pq v1.10.6
	github.com/mattn/go-sqlite3 v1.14.16
	github.com/onsi/gomega v1.20.2 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.4
	github.com/viant/afs v1.24.2
	github.com/viant/afsc v1.9.0
	github.com/viant/assertly v0.9.1-0.20220620174148-bab013f93a60
	github.com/viant/bigquery v0.2.1
	github.com/viant/cloudless v1.8.1
	github.com/viant/dsc v0.16.2 // indirect
	github.com/viant/dsunit v0.10.8
	github.com/viant/dyndb v0.1.4-0.20221214043424-27654ab6ed9c
	github.com/viant/gmetric v0.2.7-0.20220508155136-c2e3c95db446
	github.com/viant/godiff v0.4.1
	github.com/viant/parsly v0.2.0
	github.com/viant/pgo v0.10.3
	github.com/viant/scy v0.6.0
	github.com/viant/sqlx v0.8.0
	github.com/viant/structql v0.2.2
	github.com/viant/toolbox v0.34.6-0.20221112031702-3e7cdde7f888
	github.com/viant/velty v0.2.0
	github.com/viant/xdatly/types/custom v0.0.0-20230309034540-231985618fc7
	github.com/viant/xreflect v0.0.0-20230303201326-f50afb0feb0d
	github.com/viant/xunsafe v0.8.4
	golang.org/x/mod v0.9.0
	golang.org/x/oauth2 v0.7.0
	google.golang.org/api v0.114.0
	gopkg.in/go-playground/assert.v1 v1.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/viant/govalidator v0.2.1
	github.com/viant/sqlparser v0.3.1-0.20230320162628-96274e82953f
	golang.org/x/crypto v0.7.0 // indirect
)

require (
	github.com/aws/aws-sdk-go v1.44.12
	github.com/aws/aws-sdk-go-v2/config v1.18.3
	github.com/aws/aws-sdk-go-v2/service/s3 v1.33.1
	github.com/viant/structology v0.2.0
	github.com/viant/xdatly/extension v0.0.0-20230323215422-3e5c3147f0e6
	github.com/viant/xdatly/handler v0.0.0-20230619231115-e622dd6aff79
	github.com/viant/xdatly/types/core v0.0.0-20230615201419-f5e46b6b011f
)

require (
	cloud.google.com/go v0.110.0 // indirect
	cloud.google.com/go/compute v1.19.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.13.0 // indirect
	cloud.google.com/go/secretmanager v1.10.0 // indirect
	cloud.google.com/go/storage v1.29.0 // indirect
	github.com/aws/aws-sdk-go-v2 v1.18.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.13.3 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.10.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.19 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.33 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.27 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.17.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.13.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.14.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.20.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.22.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.13.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.17.5 // indirect
	github.com/aws/smithy-go v1.13.5 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.0-20210816181553-5444fa50b93d // indirect
	github.com/go-errors/errors v1.4.2 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.8.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.0 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.1 // indirect
	github.com/lestrrat-go/jwx v1.2.25 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/michael/mymodule2 v0.0.0-00010101000000-000000000000 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/viant/igo v0.1.0 // indirect
	github.com/yuin/gopher-lua v0.0.0-20221210110428-332342483e3f // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/term v0.7.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	google.golang.org/grpc v1.54.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/michael/mymodule2 => /Users/michael/myproject2/pkg

replace github.com/viant/xdatly/extension => /Users/michael/myproject2/.build/ext
```

- Check if **myproject/go.mod**:
  - uses modules with the same version like **myproject/.build/datly/go.mod**
  - uses the same version of datly you used for **~/myproject/.build** creation


## 9 Update datly in existing project

- [Ensure new version of datly](#datly)

- Delete folder ~/myproject/.build
```shell
rm -rf ~/myproject/.build
```

- Recreate ~/myproject/.build
```shell
datly initExt -p=~/myproject -n=mymodule
```

- Delete plugins if exist
```shell
rm ~/myproject/repo/dev/Datly/plugins/*
```

- Check if **myproject/pkg/go.mod** uses modules with the same version like **myproject/.build/datly/go.mod**

- Check if **myproject/go.mod** (when exists):
  - uses modules with the same version like **myproject/.build/datly/go.mod**
  - uses the same version of datly you used for **~/myproject/.build** creation  
  
**Tip:** Use command **go get example.com/pkg@v1.2.3** to install package with current version

