# Codex AI Rules for Generating DQL Files for Dynamic Tables

These rules define how Codex should generate `.dql` files for the Reader Service, using **any database-qualified table name** (e.g., `my_db.INVOICE`, `analytics.USER_LOGS`).

Everything in the `.dql` file is **optional** unless clearly requested in the user prompt — **except** for:

- The **header block**
- The `$Status` and `$Data` output parameters
- The **aliased SELECT query** using:
  ```sql
  SELECT my_table_alias.* FROM (
    SELECT ... FROM my_table
  ) my_table_alias
  ```

All other logic — pagination, predicates, joins, filters, casting, codecs, ordering, caching — should only be included **if the user explicitly asks for it**.

Additionally, each `.dql` file should be placed inside a directory named after the file **without the `.dql` extension**, derived from the lowercase version of the table name.

### ✅ File Path Convention:

For a table named `<db>.<TABLE_NAME>`:
- Convert `<TABLE_NAME>` to lowercase.
- Store the file as: `dql/<table_name_lower>/<table_name_lower>.dql`

**Examples:**

| Table Name            | File Path                 |
| --------------------- | ------------------------- |
| `my_db.INVOICE`       | `invoice/invoice.dql`     |
| `analytics.USER_LOGS` | `user_logs/user_logs.dql` |
| `sales.ORDERS`        | `orders/orders.dql`       |

### ✅ Table Alias Convention:
For any table `<TABLE_NAME>`, use its **lowercase version as the alias**.

**Examples:**

| Table Name       | Alias Used in Query   |
|------------------|-----------------------|
| `INVOICE`        | `invoice`             |
| `MY_TABLE`       | `my_table`            |
| `USER_LOGS`      | `user_logs`           |

---

## ✨ 1. Required Base Elements

These must be included in every `.dql` file:

### ✅ Header Block

```jsonc
/* {
    "URI": "/",
    "Method": "GET",
    "Description": "Fetches <columns> from <database>.<table>"
} */
```
- If no columns are provided, fallback to: "Fetches data from <database>.<table>"

### ✅ Output Parameters

```velocity
#set($_ = $Status<?>(output/status))
#set($_ = $Data<?>(output/view))
```

### ✅ Aliased Query Template

```sql
SELECT <table_alias>.*
FROM (
  SELECT <columns>
  FROM <database>.<table>
) <table_alias>
```

Use `<table_alias>` as the lowercase version of `<table>`.

---

## 🧩 2. Optional Features (Conditionally Included)

Codex should only include the following if the user prompt explicitly requests it:

| Feature      | Trigger Phrases in Prompt              | Action                                            |
| ------------ | -------------------------------------- | ------------------------------------------------- |
| Pagination   | `limit`, `page`, `pagination`          | Add `Limit`, `Page` parameters + `set_limit(...)` |
| Filtering    | `filter`, `where`, `match`, `by ...`   | Use `.WithPredicate(...)` on param declarations   |
| JWT decoding | `jwt`, `token`, `authorization`        | Use `.WithCodec(JwtClaim)` with header param      |
| Ordering     | `order by`, `sort`, `descending`, etc. | Use `order_by(view, "...")`                       |
| Joins        | `join`, `include from`, `with ...`     | Add `JOIN` clause(s) in subquery                  |
| Casting      | `cast`, `convert to type`              | Use `cast(view.col AS 'type')` in `SELECT`        |
| Caching      | `cache`, `cached`                      | Use `use_cache(view, 'cache_name')`               |

---

## 🔍 3. Metadata Validation with sqlkit
This validation should be performed using a tool called **sqlkit**.
When the prompt includes a table name with a DB prefix (e.g., `my_db.INVOICE`), the Codex agent HAS TO validate:
- That the table exists - you HAVE TO use sqlkit-dbListTables tool
- That all referenced columns exist - you HAVE TO use sqlkit-dbListColumns tool


**Validation Step:**

```sqlkit
validate table and columns for <database>.<table>
```

Codex should halt query generation and report an error if metadata validation fails.

---

## 🔁 4. Velocity Parameters (Optional)

Only insert these if a corresponding feature is requested:

### Pagination Example

```velocity
#set($_ = $Limit<int>(query/limit).Value(100))
#set($_ = $Page<int>(query/page).Value(1))
```

### Filter Example

```velocity
#set($_ = $CustomerId<[]string>(query/customer_id).WithPredicate(0, 'in', 'ag', 'CUSTOMER_ID'))
```

### JWT Codec Example

```velocity
#set($_ = $Jwt<string>(header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
```

---

## ✅ 5. Final Output Example (Basic Only)

```dql
/* {
    "URI": "/",
    "Method": "GET",
    "Description": "Fetches ID and CUSTOMER_ID from my_db.INVOICE"
} */

#set($_ = $Status<?>(output/status))
#set($_ = $Data<?>(output/view))

SELECT invoice.*
FROM (
  SELECT
    ID,
    CUSTOMER_ID
  FROM my_db.INVOICE
) invoice
```

## 6. JOINS
Always add **One‑to‑One** relation mark when one‑to‑one relation is used in the query.

---

## 📌 Summary

All `.dql` files must:

- Contain the standard header.
- Declare `$Status` and `$Data` output params.
- Use an aliased query: `SELECT <alias>.* FROM (SELECT ...) <alias>` where `<alias>` is the lowercase table name.
- Be saved under a lowercase-named directory with filename `<table>/<table>.dql`
- Validate metadata using **sqlkit** if a database-prefixed table name is used.

Everything else — pagination, filters, joins, casting, etc. — must be **explicitly requested** in the prompt. Codex must **not assume** any defaults beyond the above unless instructed.

---
