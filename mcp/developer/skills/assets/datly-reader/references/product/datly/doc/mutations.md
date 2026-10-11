## Describe the writable graph

### Queue one statement, optionally retaining its inserted identity

For a single static INSERT, UPDATE or DELETE that does not return an identity,
use the existing buffered `Execute` call. Its arguments are typed scalar
Input/Output fields in SQL placeholder order:

```sql
#package('pkg/constraints')
#setting($_ = $route('/constraints/{type}', 'PATCH'))
#define($_ = $ConstraintType<int>(path/type))
#define($_ = $Identity<int64>(output/body))

$dml.Execute("UPDATE constraints SET audience_ids = '' WHERE type = ?", $Input.ConstraintType)
```

Run `datly transcribe patch` with the normal source and destination options.
This queues the authored bulk statement rather than reading identities and
rewriting it as individual row updates. The normal root completion owns its
transaction and reports execution failures; a child does not commit or flush.
`Execute` does not populate `Identity` or expose a rows-affected result.

For a route whose write is one static INSERT, operation-based transcription can
generate a typed buffered statement handler. Declare scalar inputs and a signed
integer output destination, then author the statement in DQL:

```sql
#package('pkg/records')

#setting($_ = $route('/records/{name}', 'PATCH'))

#define($_ = $Name<string>(path/name))
#define($_ = $Identity<int64>(output/body))

$dml.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", $Output.Identity, $Input.Name)
```

Run `datly transcribe patch` with the normal source and destination options.
Generation preserves this route's lack of a request body. An optional single
SELECT graph can supply SQL-derived output shapes; its wildcard fields evolve
when the authoring schema changes and the component is regenerated. That graph
does not add a second persistence owner or an automatic response read.

The generated handler binds the invocation's existing DML capability and queues
the exact SQL and typed arguments. Root completion executes it in journal order
using the existing shared buffer and transaction. The statement's `LastInsertId`
is checked for signed overflow and assigned before output finalization,
independently of `RowsAffected`. Application input initialization and validation
and output finalization use the normal contract lifecycle. An injected child
does not flush the caller's buffer.

`ExecuteWithResult` accepts one static INSERT with one explicit VALUES row,
scalar Input/Output field arguments, and an addressable signed integer identity
destination. `Execute` accepts one static INSERT, UPDATE or DELETE with scalar
field arguments and no result destination. Both reject dynamic SQL, multiple
writes, transaction control, DDL, record sequencing,
reconciliation, identity/action policies, entity hooks and mutation tokens.
It does not allocate IDs, classify inserts versus updates, retry statements,
or query an identity afterward. Other writable graphs continue to use their
existing generated mutation policy.

Consider three tables:

| Table | Role | Relationship |
| --- | --- | --- |
| `ORDERS` | Writable parent | Full parent identity and scalar business fields |
| `ITEMS` | Writable children | Many items belong to an order through ORDER_ID |
| `ORDER_KINDS` | Auxiliary lookup | Supplies kind data; never a DML target |

```mermaid
erDiagram
    ORDER_KINDS ||--o{ ORDERS : "lookup only"
    ORDERS ||--o{ ITEMS : "mutable children"
```

The high-level PATCH generation input describes the graph and its field policies.
This is the same outer-view structure used for a reader. The selected operation
controls generation; writer annotations add behavior to its graph. Reader and
writer DQL are authored independently and can select different fields, relations
and filters.
Declare the input and output contract names alongside their destination package.
The generator derives their fields, key projections and Current-state declarations
from the graph:

```sql
#package('example.com/shop/orders/write')
#setting($_ = $input_type('OrdersInput'))
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Order>(output/body))
#setting($_ = $route('/orders', 'PATCH'))
#setting($_ = $connector('main'))
SELECT orders.*, items.*, kind.*,
       type(orders, 'Order'),
       type(items, 'Item'),
       type(kind, 'Kind'),
       lifecycle_type(orders, 'OrderLifecycle'),
       lifecycle_type(items, 'ItemLifecycle'),
       invariant(orders.WINDOW_START, 'DeliveryWindow'),
       invariant(orders.WINDOW_END, 'DeliveryWindow')
FROM (
    SELECT o.* FROM ORDERS o
) orders
LEFT JOIN (
    SELECT i.* FROM ITEMS i
) items ON items.ORDER_ID = orders.ID
LEFT JOIN (
    SELECT k.* FROM (ORDER_KINDS) k
) kind ON kind.ID = orders.KIND_ID AND 1=1
```

The database metadata supplies the real column types and keys. Use a start/end
date pair for WINDOW_START/WINDOW_END. The generator derives the request graph,
Previous reads and typed Go write support for the selected PATCH operation.
`input_type` and `output_type` name the component contracts (`OrdersInput` and
`OrdersOutput`) in the declared package. The outer `type(orders, 'Order')` and
`type(items, 'Item')` and `type(kind, 'Kind')` annotations name the individual shapes inside that
graph; they do not replace the input or output contract declarations.
`$Data<[]*Order>(output/body)` explicitly selects the typed main output holder;
`Data` is the Go output field and `body` is the writer output binding. The generated
writer populates it with the changed rows. Global `case_format('lc')` applies
Structology lower-camel casing to output names. Use this setting for routine
casing; do not add per-column JSON tags or a holder `WithTag` just to lowercase.
StructQL key projection is generated plumbing; the application should not need
to write a template loop to load Current rows.

The outer query defines the view graph: `orders`, `items` and `kind` are named
views, each supplied by its own subquery. The inner aliases `o`, `i` and `k` are
local to those SQL queries. Outer annotations address a view's projected column,
for example `orders.WINDOW_START`.

`FROM (ORDER_KINDS)` inside the `kind` view marks its source as auxiliary/nonmutating.
`AND 1=1` marks that joined relation as a single holder while keeping its real
key equality. The `items` view remains many. The generator must not infer that every joined
table is writable. Foreign-key constraints still belong to the database.

### Preserve null slots in auxiliary collections

Null collection entries are rejected by default. When an auxiliary request
collection must preserve supplied null positions, opt in on that exact view:

```sql
SELECT records.*, children.*,
       root_null_policy(records, 'skip-auxiliary'),
       nested_null_policy(children, 'skip-auxiliary')
FROM (SELECT r.* FROM (RECORDS) r) records
LEFT JOIN (SELECT c.* FROM (CHILDREN) c) children
       ON children.RECORD_ID = records.ID
```

Use `root_null_policy` for the auxiliary root collection and
`nested_null_policy` for an auxiliary child collection. Each collection needs
its own declaration; the policy does not propagate to descendants. Writable
collections and single-record relations cannot use `skip-auxiliary`.

Capture retains null positions and sparse presence for output and retry replay.
Null slots create no entity lifecycle frame, identity, sequence allocation or DML.
Supplied collection presence still participates in the parent's reconciliation
and deletion policy; `[null]` is not treated as an omitted collection. Writable
descendants retain their normal validation and allocation rules.


> Packaging boundary: Maintained writer graph, generated body/Current contract, child lifecycle and read-only auxiliary source authoring; excludes repository navigation and unrelated sections.
