# Constants and substitutions

## Typed constant parameters

Declare an application-owned constant using a typed parameter. This declaration
fragment sets a fixed integer value rather than reading a query parameter:

```sql
#define($_ = $PageSize<int>(const/PageSize).Value('100'))
```

A database SQL-source fragment can reference the parameter through a named bind:

```sql
SELECT o.ID, o.TOTAL FROM ORDERS o LIMIT :PageSize
```

Keep this SQL inside the named view of a complete reader component. Its package,
input/output types, output holder and outer row type still need explicit declarations.
Use bound parameters for SQL values; a value placeholder cannot supply a table name.

## Compile-time identifier constants

For a trusted authored table identifier, the constant setting and identifier
expansion have a separate role. These are declaration and SQL-source fragments:

```sql
#setting($_ = $const('Table', 'ORDERS'))
```

```sql
SELECT o.ID, o.TOTAL FROM $Unsafe.Table o
```

The compiler resolves the declared identifier before schema discovery. Empty
identifier values and conflicting constant declarations are errors. Do not source
identifier text from untrusted request values. Ordinary application filters remain
bound SQL values rather than identifier expansion.

The typed declaration form can also carry an identifier constant:

```sql
#define($_ = $Table<string>(const/Table).Value('ORDERS'))
```

Choose one declaration for that constant; do not assign conflicting values through
both forms. The `$Unsafe` reference is SQL template/identifier syntax, separate
from ordinary database SQL and from the outer DQL view annotations.

## Substitution-file migration boundary

The original repository-wide substitution mechanism is not yet implemented in
Datly 1.0: configured substitution URLs/profile maps, general `$Name` or `${Name}`
text expansion, and reverse substitution during rule persistence. Automatic
loading of the original `constants.yaml` dependency is also not established.
Do not describe the constant examples above as that complete pipeline or invent
a new configuration switch for it.

Documentation dictionary substitutions (`docSubstitutes`) have a narrower purpose.
They do not provide general source/configuration substitution. Until the general
pipeline is implemented and verified, declare constants explicitly and identify
substitution-file requirements as a migration gap.

## Per-instance expansion being implemented

The intended E2E/production workflow reuses constants: an instance YAML or JSON
file overrides DQL defaults, while omitted keys retain those defaults. SQL such
as `$project.ds.table` and configured resource paths keep their authored
placeholders. Expansion must happen only in transient SQL sent for column
discovery or execution, or when accessing a resource path. Expanded values must
never be persisted into DQL or generated SQL resources. This broader workflow
is still under implementation; the existing examples above do not establish it.
