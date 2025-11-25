# Guide to the Structure of a DQL File for the Reader Service
This guide shows new dql file structure for reader service.

---

## DQL Structure for Reader Service

Each `.dql` file follows this structure:
- Header as Commented JSON
- Parameters as Velocity Directives
- Query Definition

---

### 1. Header as Commented JSON

A `.dql` file must begin with a header block written in commented JSON format. This block defines the endpoint metadata.

```jsonc
/* {
    "URI":        "/",                            // required: `/`
    "Method":     "GET",                          // required: GET
    "Description": "<short summary of what this query does>"
} */
```

---

#### 1.1 🔍 Header Field Reference

| Field         | Required  | Default Value | Description                                         |
|---------------|-----------|---------------|-----------------------------------------------------|
| `URI`         | ✅        | `/`           | The route exposed to clients.                       |
| `Method`      | ✅        | `GET`         | HTTP verb for the route                             | 
| `Description` | 🚫        |               | A human-readable summary of the endpoint’s purpose  |

Field URI default logic:
 1. User explicitly provides a value → use it verbatim
 2. Otherwise, → use the generic default “/”

Field GET default logic:
1. User explicitly provides a value → use it verbatim
2. Otherwise, → use the generic default “GET”

---

### 2. Parameters as Velocity Directives

Parameters are declared using [Velocity](https://velocity.apache.org/) directive syntax:

```velocity
#set( $_ = $PARAMETER_NAME<Type>(SOURCE/LOCATION)[.OPTIONS])
```

#### 2.1 🧭 SOURCE: Origin of the Parameter

| Source      | Description                                            |
|-------------|--------------------------------------------------------|
| `query`     | HTTP request query string                              |
| `path`      | URI path parameters                                    |
| `body`      | HTTP request body derived type                         |
| `const`     | Defined constants                                      |
| `env`       | Environment variables                                  |
| `param`     | Other defined parameters                               |
| `component` | Other Datly components (e.g., ACL, plugins)            |
| `state`     | Fields from defined state                              |
| `object`    | Composite struct types                                 |
| `repeated`  | Composite slice types                                  |
| `transient` | Transient (non-persistent) data                        |
| `output`    | View response, status, metrics, or summaries           |

#### 2.2 📍 LOCATION: Specific field or path within the source

---

#### 2.3 🔍 SOURCE/LOCATION Examples

| SOURCE/LOCATION            | Description                                         |
|----------------------------|-----------------------------------------------------|
| `query/name`               | accesses the `name` parameter from the query string |
| `path/id`                  | accesses the `id` from the URI path                 |
| `body/user`                | accesses the `user` field from the request body     |
| `const/DEFAULT_VALUE`      | accesses a predefined constant                      |
| `env/ENV_VAR`              | accesses an environment variable                    |
| `param/other_param`        | accesses another parameter defined earlier          |
| `component/../acl/auth`    | accesses a nested component                         |
| `state/user_id`            | accesses stateful session data                      |
| `object/user_info`         | accesses a full object/struct                       |
| `repeated/items`           | accesses a list/slice of objects                    |
| `transient/temp_data`      | accesses transient runtime values                   |
| `output/view`              | accesses the final view or response payload         |

---

#### 2.4 ⚙️ OPTIONS

| Option                                     | Description                                                        |
|------------------------------------        |--------------------------------------------------------------------|
| `Output()`                                 | Define output parameter                                            |
| `Async()`                                  | Define asynchronous fields (e.g., UserId, UserEmail)               |
| `WithTag('tag')`                           | Attach a parameter tag                                             |
| `WithCodec('name' [, args...])`            | Define codec/transformer                                           |
| `WithPredicate('name', group [, args...])` | Attach predicates                                                  |
| `WithHandler('name' [, args...])`          | Assign handler                                                     |
| `Value(value)`                             | Default value                                                      |
| `Required()`                               | Mark as required                                                   |
| `Optional()`                               | Mark as optional                                                   |
| `Of('parent')`                             | Composite parent context                                           |
| `WithStatusCode(code)`                     | Error status override                                              |
| `Cacheable()`                              | Mark as cacheable (true by default)                                |
| `QuerySelector(viewName)`                  | Use view selectors (Fields, Criteria, etc.)                        |
| `Scope('scope name')`                      | Control parameter visibility and state injection                   |
| `When`                                     | Conditional declaration trigger                                    |

#### 2.5 💡 Parameter Definition Examples

```velocity
#set($_ = $Jwt<string>(header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Name<string>(query/name).WithPredicate(0, 'contains', 'ag', 'NAME').Optional())
#set($_ = $Id<[]int>(query/id).WithPredicate(0, 'in', 'ag', 'ID'))
#set($_ = $Auth<?>(component/../acl/auth).WithPredicate(0, 'handler', '*authorization.Agency'))
#set($_ = $Page<int>(query/page).QuerySelector(agency))

#set($_ = $Meta<?>(output/summary))
#set($_ = $Status<?>(output/status))
#set($_ = $Data<?>(output/view))
```

---

### 3. Query Definition

The `.dql` file contains SQL or DQL statements that may leverage **template variables** and **macros** to control behavior dynamically.

---

#### 3.1 DQL Configuration Functions

These helper macros are invoked inside the `SELECT` clause to adjust how the query behaves, how results are shaped, or how execution is handled.
These functions are **Velocity/DQL extensions**, not standard SQL—they're compiled into SQL at runtime.

---

##### 3.1.1 🔧 Supported Configuration Functions

| Function                                           | Description                                               |
|----------------------------------------------------|-----------------------------------------------------------|
| `set_limit(<view>, N)`                             | Sets a default row limit (e.g., for pagination)           |
| `order_by(<view>, "<col> ASC\|DESC")`              | Adds a default `ORDER BY` clause                          |
| `batch_size(<view>, N)`                            | Controls fetch batch size (for streaming or cursors)      |
| `relational_concurrency(<view>, N)`                | Sets upper limit on concurrent fetches for presplit reads |
| `cardinality(<view>, "one\|many")`                 | Declares whether a relation is one-to-one or one-to-many  |
| `match_strategy(<view>, "read_all\|read_matched")` | Controls join/filter strategy for related rows            |
| `allow_nulls(<view>[, col1, col2...])`             | Allows NULL values in specified columns                   |
| `tag(<view>.<col>, '<struct-tag>')`                | Adds a Go struct tag (e.g., `json:"field"` or `sqlx:"-"`) |
| `cast(<view>.<col> AS '<type>')`                   | Forces SQL or Go type conversion                          |
| `use_connector(<view>, '<connectorName>')`         | Overrides default connector for a view                    |
| `use_cache(<view>, '<cacheName>')`                 | Routes reads through a named cache interceptor            |
| `required(<view>.<col>)`                           | Marks a column as non-nullable in the output              |

---

##### 3.1.2 💡 Example Use

You can mix and match configuration functions in the `SELECT` clause after or alongside `*`:

```sql
SELECT *,
  set_limit(view1, 100),
  order_by(view1, "created_at DESC"),
  allow_nulls(view1),
  tag(view1.name, 'json:"name,omitempty"'),
  cast(view1.id AS 'uuid')
FROM ...
```

This gives you flexible control over limits, ordering, nullability, type casting, tagging, concurrency, and caching—**without manually altering SQL for every use case**.

---

#### 3.2 Relations (JOIN clauses)

In DQL you declare relationships using ordinary SQL `JOIN` syntax, but follow these conventions so the compiler can reason about cardinality, generate efficient code, and correctly map rows to Go structs:

| Aspect                   | Guideline                                                                             |
| ------------------------ |---------------------------------------------------------------------------------------|
| **Subquery alias**       | Each side of a join **MUST** be an aliased sub‑query, e.g. `(SELECT * FROM EMP) emp`. |
| **Default cardinality**  | If you do nothing, a `JOIN` is assumed **One‑to‑Many** (parent ⇒ children).           |
| **Override cardinality** | **One‑to‑One** relation mark: add `AND 1=1` to the `ON` clause.                       |

##### One‑to‑Many example

```sql
SELECT
  dept.*,
  emp.*
FROM (SELECT * FROM DEPARTMENT) dept
JOIN (SELECT * FROM EMPLOYEE) emp
  ON emp.DEPT_ID = dept.ID;
```

##### One‑to‑One example

```sql
SELECT
  dept.*,
  org.*
FROM (SELECT * FROM DEPARTMENT) dept
JOIN (SELECT * FROM ORGANIZATION) org
  ON org.ID = dept.ORG_ID
  AND 1=1; -- constant predicate is removed at compile‑time but signals one‑to‑one
```

##### One-to-Many and One‑to‑One example
```sql
SELECT
    dept.*
    employee.*,
    organization.*
FROM (SELECT * FROM DEPARMENT) dept
JOIN (SELECT * FROM EMP) employee ON dept.ID = employee.DEPT_ID
JOIN (SELECT * FROM ORG) organization ON organization.ID = dept.ORG_ID AND 1=1
```

These conventions let the Codex agent infer whether additional fan‑out reads are required, choose efficient read strategies, and shape the result set into embedded Go structs without manual boilerplate.

---
